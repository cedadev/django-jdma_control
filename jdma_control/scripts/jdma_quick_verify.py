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
from jdma_control.scripts.jdma_transfer import mark_migration_failed
from jdma_control.scripts.jdma_transfer import get_verify_dir
import jdma_control.backends
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level
from jdma_control.scripts.common import split_args, calculate_digest_sha256
from jdma_control.scripts.common import calculate_digest_adler32
from collections import namedtuple

VerifyFileInfo = namedtuple('VerifyFileInfo',
                            ['filepath', 'digest', 'digest_format', 'pk'])

def verify(backend_object, pr, config):
    # get the batch id
    external_id = pr.migration.external_id
    logging.info((
        "VERIFYING files in PUT request ID: {} external ID: {}"
    ).format(pr.pk, pr.migration.external_id))
    pr.stage = MigrationRequest.VERIFYING
    # get the list of files in the batch from the back end
    files = backend_object.get_files(pr, external_id)
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    if len(files) != 0:
        file_count = 0
        for archive in archive_set:
            # get a list of the filepaths
            if archive.packed:
                # not sure what to do with packed archives - but it doesn't 
                # matter as we don't have them in current JDMA
                file_list = [archive.get_archive_name()]
            else:
                for f in archive.migrationfile_set.all():
                    if f.ftype == "FILE":
                        full_path = os.path.join(pr.migration.common_path, f.path)
                        if full_path in files:
                            if int(f.size) == int(files[full_path]):
                                file_count += 1
                            else:
                                print(f"Error with file: {f.path}. expected size: {f.size}, actual size: {files[full_path]}")

    if file_count == len(files):
        print("    Verification successful")
        pr.stage = MigrationRequest.PUT_TIDY
        pr.save()
    else:
        print(f"    Verification failed, expected files: {len(files)}, actual files: {file_count}")
        pr.stage = MigrationRequest.FAILED
        pr.failure_reason = "Verification failed"
        pr.save()


def verify_files(backend_object, config):
    """Verify the files that have been uploaded to external storage and then
    downloaded back to a temporary directory."""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # these are part of a PUT request - get the list of PUT request
    prs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(locked=False)
        & Q(stage=MigrationRequest.VERIFY_PENDING)
        & Q(migration__storage__storage=storage_id)
        & ~Q(migration__external_id=None)
    )
    for pr in prs:
        if not pr:
            return
        # lock the Migration to prevent other processes acting upon it 
        if not pr.lock():
            return
        verify(backend_object, pr, config)
        pr.unlock()


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


def exit_handler(signal, frame):
    logging.info("Stopping jdma_quick_verify")
    sys.exit(0)


def run(*args):
    # setup the logging
    config = read_process_config("jdma_verify")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_quick_verify")

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
