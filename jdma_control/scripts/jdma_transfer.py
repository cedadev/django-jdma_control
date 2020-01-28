"""Functions to transfer the files in a request to (PUT) / from (GET) the
   external storage.
   Running this will change the state of the migrations
   and will invoke functions on the backed for PUT / GET operations to
   external storage.  The external_id returned from Backend.CreateUploadBatch
   will be recorded in the Migration object
"""

import os
import logging
from tarfile import TarFile
import signal
import sys
from time import sleep
import random

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, MigrationArchive
from jdma_control.models import StorageQuota
import jdma_control.backends
import jdma_control.backends.AES_tools as AES_tools
from jdma_control.scripts.common import mark_migration_failed
from jdma_control.scripts.common import restore_owner_and_group
from jdma_control.scripts.common import split_args
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import get_verify_dir, get_staging_dir, get_download_dir
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level
from jdma_control.backends.ConnectionPool import ConnectionPool

connection_pool = ConnectionPool()

def upload(backend_object, credentials, pr):
    # check we actually have some files to archive first
    if pr.migration.migrationarchive_set.all().count() == 0:
        error_string = (
            "No files in PUT or MIGRATE request: {} PUT_PENDING->FAILED"
        ).format(pr.migration.formatted_filelist()[0] + "...")
        raise Exception(error_string)
    else:
        try:
            # open a connection to the backend.  Creating the connection can account
            # for a significant portion of the run time.  So we only do it once!
            global connection_pool
            conn = connection_pool.find_or_create_connection(
                backend_object,
                mig_req=pr,
                credentials=credentials,
                mode="upload",
                uid = "PUT"
            )
            # There are two types of backends - piecewise and non-piecewise
            # piecewise (ObjectStore, FTP) allows you to upload archive by
            # archive and resume uploads.
            # Non-piecewise (ElasticTape) does not allow to upload archive by
            # archive and requires all of the files to be uploaded at once.
            # Non-piecewise also does not support resumin interrupted uploads.

            # transition the stage from PUT_PACKING->PUTTING
            pr.migration.stage = Migration.PUTTING
            pr.stage = MigrationRequest.PUTTING
            pr.migration.save()
            pr.save()
            logging.info((
                "Transition: request ID: {} external ID: {} PUT_PENDING->PUTTING, ON_DISK->PUTTING"
            ).format(pr.pk, pr.migration.external_id))

            # get the archive set here as it might change if we get it in the loop
            archive_set = pr.migration.migrationarchive_set.order_by('pk')

            if backend_object.piecewise():
                start_arch = pr.last_archive
                end_arch = pr.migration.migrationarchive_set.count()
                # loop over the archives
                for arch_num in range(start_arch, end_arch):
                    # get the archive from the archive number
                    archive = archive_set[arch_num]
                    # check whether the archive is packed
                    if archive.packed:
                        prefix = get_staging_dir(backend_object, pr)
                        file_list = [archive.get_archive_name(prefix)]
                    else:
                        prefix = pr.migration.common_path
                        # get the list of files for this archive
                        file_list = archive.get_file_names(prefix)['FILE']
                    # log message
                    logging.debug((
                        "Uploading files: {} to {}"
                    ).format(file_list, backend_object.get_name()))
                    # upload objects in filelist
                    file_inc = backend_object.upload_files(
                        conn,
                        pr,
                        prefix,
                        file_list
                    )
                    # inc archive if all files went up
                    if int(file_inc == len(file_list)):
                        pr.last_archive += 1
                        pr.save()
            else:
                # get the archive set - add all files to the filelist
                file_list = []
                for archive in archive_set:
                    # get a list of files, using the relevant prefix
                    if archive.packed:
                        prefix = get_staging_dir(backend_object, pr)
                        archive_files = [archive.get_archive_name(prefix)]
                    else:
                        prefix = pr.migration.common_path
                        # get the list of files for this archive
                        archive_files = archive.get_file_names(prefix)['FILE']
                    file_list.extend(archive_files)
                # log message
                logging.debug((
                    "Uploading files: {} to {}"
                ).format(file_list, backend_object.get_name()))
                # Upload filelist
                file_inc = backend_object.upload_files(
                    conn,
                    pr,
                    prefix,
                    file_list
                )
                # inc archive if all files went up
                if int(file_inc == len(file_list)):
                    pr.last_archive += 1
                    pr.save()

            # close the connection in the pool
            conn = connection_pool.close_connection(
                backend_object,
                pr,
                credentials,
                mode="upload",
                uid = "PUT"
            )

        except Exception as e:
            storage_name = StorageQuota.get_storage_name(
                pr.migration.storage.storage
            )
            error_string = (
                "Failed to create the upload batch for migration: {} "
                "on external storage: {}. Exception: {}"
            ).format(pr.migration.pk, storage_name, str(e))
            raise Exception(error_string)


def verify(backend_object, credentials, pr):
    """Start the verification process.  Transition from
    VERIFY_PENDING->VERIFY_GETTING and create the target directory.
    Download the batch from the backend storage to the target directory
    """
    try:
        # open a connection to the backend.  Creating the connection can account
        # for a significant portion of the run time.  So we only do it once!
        global connection_pool
        conn = connection_pool.find_or_create_connection(
            backend_object,
            mig_req=pr,
            credentials=credentials,
            mode="download",
            uid="VERIFY"
        )
        # Transition
        pr.stage = MigrationRequest.VERIFY_GETTING
        pr.save()
        logging.info((
            "Transition: request ID: {} external ID: {} VERIFY_PENDING->VERIFY_GETTING"
        ).format(pr.pk, pr.migration.external_id))

        # get the name of the verification directory
        target_dir = get_verify_dir(backend_object, pr)
        # create the target directory if it doesn't exist
        os.makedirs(target_dir, exist_ok=True)

        # for verify, we want to get the whole batch
        # get the archive set
        archive_set = pr.migration.migrationarchive_set.order_by('pk')

        # add all the files in the archive to a file_list for downloading
        file_list = []
        for archive in archive_set:
            # add files in this archive to those already added
            if archive.packed:
                archive_files = [archive.get_archive_name()]
            else:
                archive_files = archive.get_file_names()['FILE']
            file_list.extend(archive_files)

        logging.debug((
            "Downloading files to verify: {} from {}"
        ).format(file_list, backend_object.get_name()))

        backend_object.download_files(
            conn,
            pr,
            file_list = file_list,
            target_dir = target_dir
        )
        connection_pool.close_connection(
            backend_object,
            pr,
            credentials,
            mode="download",
            uid="VERIFY"
        )
    except Exception as e:
        storage_name = StorageQuota.get_storage_name(
            pr.migration.storage.storage
        )
        error_string = (
            "Failed to download for verify the migration: {} "
            "on external storage: {}. Exception: {}"
        ).format(pr.migration.pk, storage_name, str(e))
        raise Exception(error_string)


def download(backend_object, credentials, gr):
    """Start the download process.  Transition from
    GET_PENDING->GETTING and create the target directory."""

    try:
        global connection_pool
        conn = connection_pool.find_or_create_connection(
            backend_object,
            mig_req=gr,
            credentials=credentials,
            mode="download",
            uid="GET"
        )
        # Transition
        gr.stage = MigrationRequest.GETTING
        gr.save()
        logging.info((
            "Transition: request ID: {} external ID: {} GET_PENDING->GETTING"
        ).format(gr.pk, gr.migration.external_id))

        # we just (potentially) want to get a subset of archives
        archive_set, st_arch, n_arch = get_archive_set_from_get_request(gr)
        # empty file list
        file_list = []

        for arch_num in range(st_arch, n_arch):
            # determine which archive to download and stage (tar)
            archive = archive_set[arch_num]
            # get the name of the target directory
            # if the archive is packed then the name is the download directory
            # (this code currently assumes that all archives are either packed
            # or not.  This could change with some adaptation here)
            if archive.packed:
                target_dir = get_download_dir(backend_object, gr)
            else:
                # if not packed then it is the target directory from the request
                target_dir = gr.target_path

            # create the target directory if it doesn't exist
            os.makedirs(target_dir, exist_ok=True)

            # get the filelist - filter on digest and whether the file
            # has been requested
            if archive.packed:
                filt_file_list = [archive.get_archive_name()]
            else:
                filt_file_list = archive.get_file_names(
                    filter_list=gr.filelist
                )['FILE']
            # if piecewise then download bit by bit, otherwise add to file_list
            # and download at the end
            if backend_object.piecewise():
                logging.debug((
                    "Downloading files: {} from {} to {}"
                ).format(
                    filt_file_list,
                    backend_object.get_name(),
                    target_dir
                ))

                backend_object.download_files(
                    conn,
                    gr,
                    file_list = filt_file_list,
                    target_dir = target_dir
                )
            else:
                file_list.extend(filt_file_list)
        # Download all if not piecewise
        if not backend_object.piecewise():
            logging.debug((
                "Downloading files: {} from {} to {}"
            ).format(
                file_list,
                backend_object.get_name(),
                target_dir
            ))
            backend_object.download_files(
                conn,
                gr,
                file_list = file_list,
                target_dir = target_dir
            )
        # close the connection
        conn = connection_pool.close_connection(
            backend_object,
            gr,
            credentials,
            mode="download",
            uid="GET"
        )

    except Exception as e:
        storage_name = StorageQuota.get_storage_name(
            gr.migration.storage.storage
        )
        error_string = (
            "Failed to download the migration: {} "
            "on external storage: {}. Exception: {}"
        ).format(gr.migration.pk, storage_name, str(e))
        raise Exception(error_string)


def put_transfers(backend_object, key):
    """Work through the state machine to upload batches to the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the first non-locked PUT request for this backend.
    # This involves resolving two foreign keys
    pr = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(locked=False)
        & Q(migration__storage__storage=storage_id)
        & Q(stage__in=[
            MigrationRequest.PUT_PENDING,
            MigrationRequest.VERIFY_PENDING,
        ])
    ).first()

    # .first() returns None when no requests that match the filter are found
    if not pr:
        return
    # lock the Migration to prevent other processes acting upon it
    if not pr.lock():
        return
    # determine the credentials for the user - decrypt if necessary
    if pr.credentials != {}:
        credentials = AES_tools.AES_decrypt_dict(key, pr.credentials)
    else:
        credentials = {}

    # Check whether data is being put to external storage
    if pr.stage == MigrationRequest.PUT_PENDING:
        # create the batch on this instance, next time the script is run
        # the archives will be created as tarfiles
        try:
            upload(backend_object, credentials, pr)
        except Exception as e:
            # Something went wrong, set FAILED and failure_reason
            mark_migration_failed(pr, str(e), e)
    # check if data is now on external storage and should be pulled
    # back for verification
    elif pr.stage == MigrationRequest.VERIFY_PENDING:
        # pull back the data from the external storage
        try:
            verify(backend_object, credentials, pr)
        except Exception as e:
            # Something went wrong, set FAILED and failure_reason
            mark_migration_failed(pr, str(e), e)
    # unlock
    pr.unlock()


def restore_owner_and_group_on_get(backend_object, gr):
    # change the owner, group and permissions of the file to match that
    # of the original from the user query
    restore_owner_and_group(gr.migration, gr.target_path)

    # if we reach this point then the restoration has finished.
    # next stage is tidy up
    logging.info((
        "Transition: request ID: {}: GET_RESTORE->GET_TIDY"
    ).format(gr.pk))
    gr.stage = MigrationRequest.GET_TIDY
    gr.last_archive = 0
    gr.save()


def get_transfers(backend_object, key):
    """Work through the state machine to download batches from the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # get the GET requests which are queued (GET_PENDING) for this backend
    gr = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(locked=False)
        & Q(migration__storage__storage=storage_id)
        & Q(stage__in=[
              MigrationRequest.GET_PENDING,
              MigrationRequest.GETTING,
              MigrationRequest.GET_RESTORE,
          ])
    ).first()

    # .first() returns None when no requests that match the filter are found
    if not gr:
        return
    # lock the Migration to prevent other processes acting upon it
    if not gr.lock():
        return
    # determine the credentials for the user - decrypt if necessary
    if gr.credentials != {}:
        credentials = AES_tools.AES_decrypt_dict(key, gr.credentials)
    else:
        credentials = {}

    if gr.stage == MigrationRequest.GET_PENDING:
        # we might have to do something here, like create a download batch
        # for elastic tape.  Also create the directory and transition the
        # state
        try:
            download(backend_object, credentials, gr)
        except Exception as e:
            # Something went wrong, set FAILED and failure_reason
            mark_migration_failed(gr, str(e), e, upload_mig=False)

    elif gr.stage == MigrationRequest.GETTING:
        pass

    elif gr.stage == MigrationRequest.GET_RESTORE:
        # restore the file permissions
        try:
            restore_owner_and_group_on_get(backend_object, gr)
        except Exception as e:
            mark_migration_failed(gr, str(e), e, upload_mig=False)
    gr.unlock()


def delete(backend_object, credentials, dr):
    """Delete the batch, taking turns to delete a single archive at once."""
    # open a connection to the backend.  Creating the connection can account
    # for a significant portion of the run time.  So we only do it once!
    global connection_pool
    conn = connection_pool.find_or_create_connection(
        backend_object,
        mig_req=dr,
        credentials=credentials,
        mode="delete",
        uid="DELETE"
    )
    try:
        # Transition
        logging.info((
            "Transition: request ID: {} external ID: {} DELETE_PENDING->DELETING"
        ).format(dr.pk, dr.migration.external_id))

        dr.migration.stage = Migration.DELETING
        dr.stage = MigrationRequest.DELETING
        dr.migration.save()
        dr.save()
        logging.debug((
            "Deleting batch: {} from {}"
        ).format(dr.migration.external_id, backend_object.get_name()))

        backend_object.delete_batch(
            conn,
            dr,
            dr.migration.external_id,
        )
    except Exception as e:
        raise(e)


def delete_transfers(backend_object, key):
    """Work through the state machine to delete batches from the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # get the DELETE requests which are queued (DELETE_PENDING) for this backend
    dr = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(locked=False)
        & Q(migration__storage__storage=storage_id)
        & Q(stage__in=[
            MigrationRequest.DELETE_PENDING,
            MigrationRequest.DELETING
          ])
    ).first()

    # .first() returns None when no requests that match the filter are found
    if not dr:
        return
    # lock the Migration to prevent other processes acting upon it
    if not dr.lock():
        return
    # find the associated PUT or MIGRATE migration request
    # if there is one - if not, set put_req to None
    # there will not be a migration request if the migration has completed
    # as the migration request is deleted when a PUT or MIGRATE completes
    try:
        put_req = MigrationRequest.objects.get(
            (Q(request_type=MigrationRequest.PUT) |
             Q(request_type=MigrationRequest.MIGRATE))
            & Q(migration=dr.migration)
            & Q(migration__storage__storage=storage_id)
        )
    except:
        put_req = None

    # determine the credentials for the user - decrypt if necessary
    if dr.credentials != {}:
        credentials = AES_tools.AES_decrypt_dict(key, dr.credentials)
    else:
        credentials = {}

    # switch on the state machine status
    if dr.stage == MigrationRequest.DELETE_PENDING:
        try:
            # only try to do the delete if some files have been uploaded!
            # and the external id is not None
            if ((put_req and put_req.stage > MigrationRequest.PUT_PACKING and
                 dr.migration.external_id is not None)
               or (dr.migration.stage == Migration.ON_STORAGE)
            ):
                delete(backend_object, credentials, dr)
            else:
                # transition to DELETE_TIDY if there are no files to delete
                dr.stage = MigrationRequest.DELETE_TIDY
                logging.info((
                    "Transition: request ID: {} external_id {}: DELETING->DELETE_TIDY"
                ).format(dr.pk, dr.migration.external_id))
                dr.save()
            del_count += 1
        except Exception as e:
            # Something went wrong, set FAILED and failure_reason
            mark_migration_failed(dr, str(e), e, upload_mig=False)

    elif dr.stage == MigrationRequest.DELETING:
    # in the process of deleting
        pass
    # unlock
    dr.unlock()

def process(backend_object, key):
    """Run the transfer processes on a backend.
    Keep a running total of whether any processes were run.
    If they weren't then put the daemon to sleep for a minute to prevent the
    database being hammered"""
    put_transfers(backend_object, key)
    get_transfers(backend_object, key)
    delete_transfers(backend_object, key)

def shutdown_handler(signum, frame):
    logging.info("Stopping jdma_transfer")
    sys.exit(0)


def run_loop(backend_objects):
    # Run the main loop over and over
    try:
        # read the decrypt key
        key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)
        for backend_object in backend_objects:
            process(backend_object, key)
    except SystemExit:
        for backend_object in backend_objects:
            backend_object.exit()
        sys.exit(0)
    except Exception as e:
        # catch all exceptions as we want this to run in a loop for all
        # backends and transfers - we don't want one transfer to crash out
        # the transfer daemon with a single bad transfer!
        # output the exception to the log so we can see what went wrong
        logging.error(str(e))


def run(*args):
    # setup the logging
    # setup exit signal handling
    global connection_pool
    config = read_process_config("jdma_transfer")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_transfer")

    # remap signals to shutdown handler which in turn calls sys.exit(0)
    # and raises SystemExit exception
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # process the arguments
    arg_dict = split_args(args)

    # create a list of backend objects to run process on
    # are we running one backend or many?
    backend_objects = []
    if "backend" in arg_dict:
        backend = arg_dict["backend"]
        # one backend
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend_class = jdma_control.backends.get_backend_from_id(backend)
            backend_objects.append(backend_class())
    else:
        # all the backends
        for backend in jdma_control.backends.get_backends():
            backend_objects.append(backend())

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
            run_loop(backend_objects)
            # add a random amount of time to prevent(?) race conditions
            sleep(5 + random.random())
    else:
        run_loop(backend_objects)
