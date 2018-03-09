"""Functions to tidy up after the JDMA has migrated data to external storage.
   It will do the following:
   1.  Delete the verification directory and all its contents
   2.  Delete the file list in /jdma_file_lists
   3.  Delete the digest in /jdma_file_lists
   4.  Delete the original directory and all its contents (!)
   5.  Remove the migration request (but leave the migration)
   Running this will not change the state of any of the migrations.
"""

import os
import logging
import shutil

from django.db.models import Q

from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging

import jdma_control.backends

def remove_verification_files(backend_object):
    """Remove those temporary files that have been created in the verification step"""
    # these occur during a PUT request
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # only do it if the files are on external storage
        if pr.migration.stage == Migration.ON_STORAGE:
            # get the directory that the temporary files are in
            batch_id = pr.migration.external_id
            # get the temporary directory
            verify_dir = os.path.join(backend_object.VERIFY_DIR, "batch{}".format(batch_id))
            # remove the directory
            if os.path.isdir(verify_dir):
                shutil.rmtree(verify_dir)
                logging.info("TIDY: deleting directory " + verify_dir)
            else:
                logging.error("TIDY: cannot find directory " + verify_dir)


def remove_original_files(backend_object):
    """Remove the original files.  This is the whole point of the migration!"""
    # these occur during a PUT request
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # only do it if the files are on external storage
        if pr.migration.stage == Migration.ON_STORAGE:
            # loop over the files in the filelist
            for fd in pr.migration.filelist:
                # check whether it's a directory: walk if it is
                if os.path.isdir(fd):
                    # delete the whole directory!
                    try:
                        shutil.rmtree(fd)
                        logging.info((
                            "TIDY: deleting directory {}"
                        ).format(fd))
                    except Exception as e:
                        logging.info((
                            "TIDY: could not delete directory {} : {}"
                        ).format(fd, str(e)))
                else:
                    try:
                        os.unlink(fd)
                        logging.info((
                            "TIDY: deleting file {}"
                        ).format(fd))
                    except Exception as e:
                        logging.info((
                            "TIDY: could not delete file {} : {}"
                        ).format(fd, str(e)))


def remove_put_requests(backend_object):
    """Remove the put requests that are ON_STORAGE"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # only do it if the files are on external storage
        if pr.migration.stage == Migration.ON_STORAGE:
            logging.info("TIDY: deleting PUT request {}".format(pr.pk))
            pr.delete()


def remove_get_requests(backend_object):
    """Remove the get requests that are ON_DISK"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend.
    # This involves resolving two foreign keys
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
    )
    for gr in get_reqs:
        # only do it if the files are on external storage
        if gr.migration.stage == Migration.ON_STORAGE:
            logging.info("TIDY: deleting GET request {}".format(gr.pk))
            gr.delete()


def run():
    setup_logging(__name__)
    # these are individual loops to aid debugging and so we can turn them
    # on / off if we wish
    # loop over all backends
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        remove_verification_files(backend_object)
        remove_original_files(backend_object)
        remove_put_requests(backend_object)
        remove_get_requests(backend_object)
