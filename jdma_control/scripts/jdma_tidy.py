"""Functions to tidy up after the JDMA has migrated data to tape.
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

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.scripts.jdma_lock import setup_logging

import jdma_control.backends

def remove_verification_files():
    """Remove those temporary files that have been created in the verification step"""
    # these occur during a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_STORAGE:
            # get the directory that the temporary files are in
            batch_id = pr.migration.external_id
            # get the temporary directory
            verify_dir = os.path.join(settings.JDMA_BACKEND_OBJECT.VERIFY_DIR, "batch{}".format(batch_id))
            # remove the directory
            if os.path.isdir(verify_dir):
                shutil.rmtree(verify_dir)
                logging.info("TIDY: deleting directory " + verify_dir)
            else:
                logging.error("TIDY: cannot find directory " + verify_dir)


def remove_original_files():
    """Remove the original files.  This is the whole point of the migration!"""
    # these occur during a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_STORAGE:
            if os.path.isdir(pr.migration.original_path):
                shutil.rmtree(pr.migration.original_path)
                logging.info("TIDY: deleting directory " + pr.migration.original_path)
            else:
                logging.error("TIDY: cannot delete directory " + pr.migration.original_path)


def remove_put_requests():
    """Remove the put requests that are ON_STORAGE"""
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_STORAGE:
            logging.info("TIDY: deleting PUT request {}".format(pr.pk))
            pr.delete()


def remove_get_requests():
    """Remove the get requests that are ON_DISK"""
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    for gr in get_reqs:
        # only do it if the files are on tape
        if gr.stage == MigrationRequest.ON_DISK and gr.migration.stage == Migration.ON_STORAGE:
            logging.info("TIDY: deleting GET request {}".format(gr.pk))
            gr.delete()


def run():
    setup_logging(__name__)
    # these are individual loops to aid debugging and so we can turn them
    # on / off if we wish
    remove_verification_files()
    remove_original_files()
    remove_put_requests()
    remove_get_requests()
