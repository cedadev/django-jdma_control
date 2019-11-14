"""Functions to verify files that have been migrated to external storage.
   These files have been put on external storage and then (temporarily) pulled
   back to disk before being verified by calculating the SHA256 digest and
   comparing it to the digest that was calculated (in jdma_transfer) before it
   was uploaded to external storage.
   Running this will change the state of the migrations:
     VERIFYING->ON_STORAGE
"""

import os
import sys
import logging
import signal
from time import sleep
import random
from multiprocessing import Process

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.models import StorageQuota, MigrationFile
from jdma_control.scripts.jdma_lock import calculate_digest
from jdma_control.scripts.jdma_transfer import mark_migration_failed
from jdma_control.scripts.jdma_transfer import get_verify_dir
import jdma_control.backends
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level
from jdma_control.scripts.common import split_args


def get_permissions_string(p):
    # this is unix permissions
    is_dir = 'd'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = oct(p)[-3:]
    return is_dir + ''.join(dic.get(x,x) for x in perm)

def verify_list_of_files(list_of_files, pr):
    """Verify a list of files by recalculating the digest.
    list_of_files is a list of tuples (file_path, digest)
    where digest has been calculated previously"""

    for file_info in list_of_files:
        try:
            # calculate the new digest
            new_digest = calculate_digest(file_info[0])
            # get the stored digest - this will depend if the archive
            # is packed or not
            # check that the digests match
            if new_digest != file_info[1]:
                failure_reason = (
                    "VERIFY: file or archive {} has a different digest."
                ).format(file_info[0])
                mark_migration_failed(pr, failure_reason)
        except Exception as e:
            # check the file exists - if it doesn't then set the stage to
            # FAILED and write that the file couldn't be found in the
            # failure_reason
            failure_reason = (
                "VERIFY: file or archive {} failed: {}"
            ).format(file_info[0], str(e))
            mark_migration_failed(pr, failure_reason)


def verify(backend_object, pr, config):
    # check whether locked
    if pr.locked:
        return
    # lock the migration
    pr.lock()
    # get the batch id
    external_id = pr.migration.external_id
    # get the temporary directory
    verify_dir = get_verify_dir(backend_object, pr)
    # loop over the MigrationArchives that belong to this Migration
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    # use last_archive to enable restart of verification
    st_arch = 0 # pr.last_archive
    n_arch = archive_set.count()
    # build a list of files to verify, along with the digest calculated on
    # pack / upload
    file_and_digest_list = []
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # get a list of the filepaths
        if archive.packed:
            file_list = [archive.get_archive_name()]
        else:
            file_list = archive.get_file_names()['FILE']
        for file_path in file_list:
            # filename is concatenation of verify_dir and the original
            # file path
            verify_file_path = os.path.join(
                verify_dir,
                file_path
            )
            if archive.packed:
                stored_digest = archive.digest
            else:
                file_obj = MigrationFile.objects.get(
                    path=file_path,
                    archive__migration=pr.migration
                )
                stored_digest = file_obj.digest

            # add the filename, digest and archive index to the list
            file_and_digest_list.append((
                verify_file_path,
                stored_digest,
                archive.pk
            ))

    # now create a number of threads to check the digests
    if len(file_and_digest_list) > 0:
        n_threads = config["THREADS"]
        processes = []
        for tn in range(0, n_threads):
            local_list = file_and_digest_list[tn::n_threads]
            p = Process(
                target = verify_list_of_files,
                args = (local_list, pr)
            )
            p.start()
            processes.append(p)

        # block here until all threads have completed
        for p in processes:
            p.join()

    # if we reach this part without FAILING then the batch has verified
    # successfully and we can transition to PUT_TIDY, ready for the
    # tidy up process
    if pr.stage != MigrationRequest.FAILED:
        pr.stage = MigrationRequest.PUT_TIDY
        # reset last archive
        pr.last_archive = 0
        # unlock
        pr.locked = False
        pr.save()
        logging.info((
            "Transition: request ID: {} external ID: {} VERIFYING->PUT_TIDY"
        ).format(pr.pk, pr.migration.external_id))

def verify_files(backend_object, config):
    """Verify the files that have been uploaded to external storage and then
    downloaded back to a temporary directory."""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # these are part of a PUT request - get the list of PUT request
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.VERIFYING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        verify(backend_object, pr, config)

def exit_handler(signal, frame):
    logging.info("Stopping jdma_verify")
    sys.exit(0)


def run_loop(backend, config):
    if backend is None:
        for backend in jdma_control.backends.get_backends():
            backend_object = backend()
            verify_files(backend_object, config)
    else:
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            backend_object = backend()
            verify_files(backend_object, config)

def run(*args):
    # setup the logging
    config = read_process_config("jdma_verify")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_verify")

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
            sleep(5 + random.random())
    else:
        run_loop(backend, config)
