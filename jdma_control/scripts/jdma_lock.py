"""Functions to transition a migration request from:
ON_DISK -> PUT_PENDING     - locks the directory for migration by changing
   the owner to root / jdma user
ON_STORAGE -> GET_PENDING  - create the target directory, and lock it by
   changing the owner again

This has become a less simple program that could be run less frequently than
previously designed, e.g. every hour even.
It is the digest calculation that takes time so we could split that into a
different part of the state machine
"""

import logging
import os
import subprocess
from operator import attrgetter
import signal, sys
from time import sleep, time
from multiprocessing import Process, Queue

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.models import MigrationArchive, MigrationFile
import jdma_control.backends
from jdma_control.backends.Backend import get_backend_object

from jdma_control.scripts.common import *
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level

def joins(path, list_of_paths):
    return_list = []
    for f in list_of_paths:
        return_list.append(os.path.join(path, f))
    return return_list


def get_info_and_lock_file(user_name, files_dirs_list, q):
    file_infos = []
    for file_dir in files_dirs_list:
        # get the info for the file
        file_info = get_file_info_tuple(
            file_dir,
        )
        # 1. change the owner of the file to be root
        # 2. change the read / write permissions to be user-only
        subprocess.call([
            "/usr/bin/sudo",
            "/bin/chown",
            "root:root",
            file_dir
        ])
        subprocess.call([
            "/usr/bin/sudo",
            "/bin/chmod",
            "700",
            file_dir
        ])
        file_infos.append(file_info)
    q.put(file_infos)


def lock_put_migration(pr, config):
    """Move this to it's own function so that it can be used in threading.
    """
    # loop over the files or directories - copy full paths for the files and
    # directories into a list
    file_infos = []
    files_dirs_list = []
    for fd in pr.filelist:
        # check whether it's a directory: walk if it is
        if os.path.isdir(fd):
            # create the file list of all the files and directories under
            # the original directory
            # don't follow symbolic links!
            user_file_list = os.walk(fd, followlinks=False)
            for root, dirs, files in user_file_list:
                # add the files
                files_dirs_list.extend(joins(root, files))
                # add the directories
                files_dirs_list.extend(joins(root, dirs))
                # append the root if not in files_dirs_list
                if root not in files_dirs_list:
                    files_dirs_list.append(root)
        else:
            files_dirs_list.extend(fd)

    # find the common path for the file_infos.filepath
    pr.migration.common_path = os.path.commonprefix(files_dirs_list)
    # get the fileinfo for the common path
    cp_file_info = get_file_info_tuple(pr.migration.common_path)

    pr.migration.common_path_user_id = cp_file_info.unix_user_id
    pr.migration.common_path_group_id = cp_file_info.unix_group_id
    pr.migration.common_path_permission = cp_file_info.unix_permission

    # now loop over the file list and get the fileinfo - this is
    # parallelised as it involves computing a checksum and changing file
    # permissions so is IO bound
    n_threads = config["THREADS"]
    processes = []
    user_name = pr.migration.user.name
    for tn in range(0, n_threads):
        local_files_dirs = files_dirs_list[tn::n_threads]
        q = Queue()
        p = Process(
            target=get_info_and_lock_file,
            args=(user_name, local_files_dirs, q)
        )
        p.start()
        processes.append((p, q))

    # block here until all threads have completed
    for p in processes:
        p[0].join()
    # get the file lists from the queues
    for p in processes:
        file_infos.extend(p[1].get())

    # 1. change the owner of the common_path directory to be root
    # 2. change the read / write permissions to be user-only
    subprocess.call([
        "/usr/bin/sudo",
        "/bin/chown",
        "-R",
        "root:root",
        pr.migration.common_path
    ])
    subprocess.call([
        "/usr/bin/sudo",
        "/bin/chmod",
        "-R",
        "700",
        pr.migration.common_path
    ])

    # sort the file_infos based on size using attrgetter
    # this will group all the small files together
    # we will take files from the back, so set descending to True
    file_infos.sort(key=attrgetter('size'), reverse=True)

    # get the backend object minimum size
    backend_object = get_backend_object(pr.migration.storage.get_name())

    # keep adding files to MigrationArchives until there are none left
    # (when current file < 0)
    n_current_file = len(file_infos) - 1

    # delete all migration archives for this migration
    if pr.migration.migrationarchive_set.count() > 0:
         pr.migration.migrationarchive_set.all().delete()

    # keep tabs on the total size
    total_size = 0

    while n_current_file >= 0:
        # create a new MigrationArchive
        mig_arc = MigrationArchive()
        # assign the migration, copy from the MigrationRequest
        mig_arc.migration = pr.migration
        # determine whether it should be packed or not
        mig_arc.packed = backend_object.pack_data()
        mig_arc.save()
        # now create the files - while there are files left and the current
        # archive size is less than the minimum object size for the backend
        current_size = 0
        while (n_current_file >= 0 and
                current_size < backend_object.minimum_object_size()):
            # create the migration file using the fileinfo pointed to by
            # n_current_file
            mig_file = MigrationFile()
            fileinfo = file_infos[n_current_file]
            # add the size to the current archive size
            current_size += fileinfo.size
            # fill in the details
            # the filepath has the commonprefix removed
            mig_file.path = fileinfo.filepath.replace(
                pr.migration.common_path, ""
            )
            mig_file.size = fileinfo.size
            mig_file.digest = fileinfo.digest
            mig_file.unix_user_id = fileinfo.unix_user_id
            mig_file.unix_group_id = fileinfo.unix_group_id
            mig_file.unix_permission = fileinfo.unix_permission
            mig_file.archive = mig_arc

            # add the size to the total size for the migration - to check
            # against the quota
            total_size += fileinfo.size
            # go to the next file (going backwards through a descending
            # sorted list remember!)
            n_current_file -= 1
            # don't add the file if it's empty after replacing the common_path
            # with the null string (e.g. this happens with the root directory)
            if len(mig_file.path) > 0:
                # remove the slash if it is the first character as this causes
                # os.path.join to treat it as the root
                if mig_file.path[0] == "/":
                    mig_file.path = mig_file.path[1:]
                # save the size
                mig_file.archive.size = current_size
                # save the Migration File
                mig_file.save()
                logging.info("PUT: Added file: " + mig_file.path)
        # save the migration archive
        mig_arc.save()

    # check whether the total size + the quota_used is greater than the
    # quota_size
    storage = pr.migration.storage
    if total_size + storage.quota_used > storage.quota_size:
        error_string = ((
            "Moving files to external storage: {} would cause the quota for the"
            " workspace: {} to be exceeded.\n"
            " Current used quota: {} \n"
            " Quota size: {} \n"
            " Size of files in request: {} \n"
        ).format(StorageQuota.get_storage_name(storage.storage),
                 pr.migration.workspace.workspace,
                 storage.quota_formatted_used(),
                 storage.quota_formatted_size(),
                 sizeof_fmt(total_size)))
        mark_migration_failed(pr, error_string, None)
    else:
        # set the MigrationRequest stage to be PUT_PACKING and the
        # Migration stage to be PUTTING
        pr.stage = MigrationRequest.PUT_PACKING
        pr.migration.stage = Migration.PUTTING
        pr.migration.save()
        pr.save()


def lock_put_migrations(backend_object, config):
    """Lock the directories that are going to be put to external storage.
       Also build the MigrationFiles entries from walking the directories
       This is to ensure that the user doesn't write any more data to them
       while the external storage write is ongoing.
    """
    # get the storage id for the backend
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # get the list of PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.PUT_START)
        & Q(migration__storage__storage=storage_id)
    )
    # for each PUT request get the Migration and determine if the type of the
    # Migration is ON_DISK
    for pr in put_reqs:
        # check if locked in the database
        if pr.locked:
            continue
        # lock the migration in the database
        pr.lock()
        lock_put_migration(pr, config)
        pr.unlock()


def lock_get_migration(gr):
    """Lock a single get migration, so we can multiprocess."""
    # check locked
    # if it's on external storage then:
    # 1. Make the directory if it doesn't exist
    # 2. change the owner of the directory to be root
    # 3. change the read / write permissions to be user-only
    if not os.path.isdir(gr.target_path):
        subprocess.call(["/usr/bin/sudo", "/bin/mkdir",
                         gr.target_path])
    subprocess.call(["/usr/bin/sudo", "/bin/chown", "root:root",
                    gr.target_path])
    subprocess.call(["/usr/bin/sudo", "/bin/chmod", "700",
                    gr.target_path])
    # set the migration stage to be GET_PENDING
    gr.stage = MigrationRequest.GET_PENDING
    gr.save()
    logging.info("GET: Locked directory: " + gr.target_path)
    logging.info((
        "Transition: request ID: {} GET_START->GET_PENDING"
    ).format(gr.pk))


def lock_get_migrations(backend_object):
    """Lock the directories that the targets for recovering data from external
    storage.  This is to ensure that there aren't any filename conflicts.
    """
    # get the storage id for the backend
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # get the list of GET requests
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(stage=MigrationRequest.GET_START)
        & Q(migration__storage__storage=storage_id)
    )
    for gr in get_reqs:
        if gr.locked:
            continue
        gr.lock()
        lock_get_migration(gr)
        gr.unlock()


def lock_delete_migration(backend_object, dr):
    # lock this migration request as well
    # find the associated PUT, MIGRATE and GET migration requests and lock
    # them
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    other_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE)
        | Q(request_type=MigrationRequest.GET))
        & Q(migration=dr.migration)
        & Q(migration__storage__storage=storage_id)
    )
    # lock the associated migration(s)
    for otr in other_reqs:
        otr.lock()
    # transition to DELETE_PENDING
    dr.stage = MigrationRequest.DELETE_PENDING
    dr.save()
    logging.info("DELETE: Locked migration: {}".format(dr.migration.pk))
    logging.info((
        "Transition: request ID: {} DELETE_START->DELETE_PENDING"
    ).format(dr.pk))


def lock_delete_migrations(backend_object):
    # get the storage id for the backend
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # get the list of GET requests
    del_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(stage=MigrationRequest.DELETE_START)
        & Q(migration__storage__storage=storage_id)
    )
    # set any associated MigrationRequests - i.e. acting on the same Migration
    # to locked
    for dr in del_reqs:
        if dr.locked:
            continue
        dr.lock()
        lock_delete_migration(backend_object, dr)
        dr.unlock()


def process(backend, config):
    backend_object = backend()
    lock_put_migrations(backend_object, config)
    lock_get_migrations(backend_object)
    lock_delete_migrations(backend_object)


def exit_handler(signal, frame):
    logging.info("Stopping jdma_lock")
    sys.exit(0)


def run_loop(backend, config):
    # moved this to a function so we can call a one-shot version
    if backend is None:
        for backend in jdma_control.backends.get_backends():
            process(backend, config)
    else:
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend, config)


def run(*args):
    """Entry point for the Django script run via ``./manage.py runscript``
    optionally pass the backend_id in as an argument
    """
    config = read_process_config("jdma_lock")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )

    logging.info("Starting jdma_lock")

    # setup exit signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # process the arguments
    arg_dict = split_args(args)
    if "backend" in arg_dict:
        backend = arg_dict["backend"]
    else:
        backend = None

    # decide whether to run as a daemon
    if "daemon" in arg_dict:
        if arg_dict["daemon"].lower() == "true":
            daemon = True
        else:
            daemon = False
    else:
        daemon = False

    # run as a daemon or one shot
    if daemon:
        # loop this indefinitely until the exit signals are triggered
        while True:
            run_loop(backend, config)
            sleep(5)
    else:
        run_loop(backend, config)
