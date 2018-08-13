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

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.models import MigrationArchive, MigrationFile
from jasmin_ldap.core import *
from jasmin_ldap.query import *
import jdma_control.backends
from jdma_control.backends.Backend import get_backend_object

from jdma_control.scripts.common import *


def lock_migration(pr, conn):
    """Move this to it's own function so that it opens the possibility of
    threading later on
    """
    # loop over the files or directories - copy full paths and other info
    # about the file into a list
    fileinfos = []
    for fd in pr.filelist:
        # check whether it's a directory: walk if it is
        if os.path.isdir(fd):
            # create the file list of all the files and directories under
            # the original directory
            # don't follow symbolic links!
            user_file_list = os.walk(fd, followlinks=False)
            for root, dirs, files in user_file_list:
                # files
                for fl in files:
                    # get the full path and append to the list
                    filepath = os.path.join(root, fl)
                    # get the info for the file
                    file_info = get_file_info_tuple(
                        filepath,
                        pr.migration.user.name,
                        conn
                    )
                    # append
                    fileinfos.append(file_info)
                # directories
                for dl in dirs:
                    filepath = os.path.join(root, dl)
                    # get the info for the file
                    file_info = get_file_info_tuple(
                        filepath,
                        pr.migration.user.name,
                        conn
                    )
                    # append a directory
                    if (file_info.is_dir):
                        fileinfos.append(file_info)
                # root
                file_info = get_file_info_tuple(
                    root,
                    pr.migration.user.name,
                    conn
                )
                # append the root if not in fileinfos
                if (file_info.is_dir and file_info not in fileinfos):
                    fileinfos.append(file_info)
        else:
            # get the info for the file
            file_info = get_file_info_tuple(
                fd,
                pr.migration.user.name,
                conn
            )
            # append to the list of files
            fileinfos.append(file_info)

    # find the common path for the fileinfos.filepath
    pr.migration.common_path = os.path.commonprefix(
        [f.filepath for f in fileinfos]
    )
    # get the fileinfo for the common path
    cp_file_info = get_file_info_tuple(
        pr.migration.common_path,
        pr.migration.user.name,
        conn
    )

    pr.migration.common_path_user_id = cp_file_info.unix_user_id
    pr.migration.common_path_group_id = cp_file_info.unix_group_id
    pr.migration.common_path_permission = cp_file_info.unix_permission

    # change each file / directory in the fileinfos to be root
    for f in fileinfos:
        # 1. change the owner of the file to be root
        # 2. change the read / write permissions to be user-only
        subprocess.call([
            "/usr/bin/sudo",
            "/bin/chown",
            "root:root",
            f.filepath
        ])
        subprocess.call([
            "/usr/bin/sudo",
            "/bin/chmod",
            "700",
            f.filepath
        ])

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

    # sort the fileinfos based on size using attrgetter
    # this will group all the small files together
    # we will take files from the back, so set descending to True
    fileinfos.sort(key=attrgetter('size'), reverse=True)

    # get the backend object minimum size
    backend_object = get_backend_object(pr.migration.storage.get_name())

    # keep adding files to MigrationArchives until there are none left
    # (when current file < 0)
    n_current_file = len(fileinfos) - 1

    # keep tabs on the total size
    total_size = 0

    while n_current_file >= 0:
        # create a new MigrationArchive
        mig_arc = MigrationArchive()
        # assign the migration, copy from the MigrationRequest
        mig_arc.migration = pr.migration
        # determine whether it should be packed or not
        mig_arc.packed = backend_object.pack_data()
        # save the migration archive
        mig_arc.save()

        # now create the files - while there are files left and the current
        # archive size is less than the minimum object size for the backend
        current_size = 0
        while (n_current_file >= 0 and
                current_size < backend_object.minimum_object_size()):
            # create the migration file using the fileinfo pointed to by
            # n_current_file
            mig_file = MigrationFile()
            fileinfo = fileinfos[n_current_file]
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


def lock_put_filelists(backend_object):
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
    # create the required ldap server pool, do this just once to
    # improve performance
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                              settings.JDMA_LDAP_REPLICAS)
    conn = Connection.create(ldap_servers)
    # for each PUT request get the Migration and determine if the type of the
    # Migration is ON_DISK
    for pr in put_reqs:
        # check if locked in the database
        if pr.locked:
            continue
        # lock the migration in the database
        pr.lock()
        lock_migration(pr, conn)
        pr.unlock()
    conn.close()


def lock_get_directories(backend_object):
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
    # for each GET request get the Migration and determine if the type of the
    # Migration is ON_TAP
    for gr in get_reqs:
        # check locked
        if gr.locked:
            continue
        gr.lock()
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
        # unlock
        gr.locked = False
        gr.save()
        logging.info("GET: Locked directory: " + gr.target_path)
        logging.info((
            "Transition: request ID: {} GET_START->GET_PENDING"
        ).format(gr.pk))


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
        # lock this migration request as well
        if dr.locked:
            continue
        dr.lock()
        # find the associated PUT, MIGRATE and GET migration requests and lock
        # them
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
        dr.locked = False
        dr.save()
        logging.info("DELETE: Locked migration: {}".format(dr.migration.pk))
        logging.info((
            "Transition: request ID: {} GET_START->GET_PENDING"
        ).format(dr.pk))


def process(backend):
    backend_object = backend()
    lock_put_filelists(backend_object)
    lock_get_directories(backend_object)
    lock_delete_migrations(backend_object)


def run(*args):
    """Entry point for the Django script run via ``./manage.py runscript``
    optionally pass the backend_id in as an argument
    """
    setup_logging(__name__)
    if len(args) == 0:
        for backend in jdma_control.backends.get_backends():
            process(backend)
    else:
        backend = args[0]
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend)
