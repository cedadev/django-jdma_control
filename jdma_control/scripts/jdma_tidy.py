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

import jdma_control.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_lock import setup_logging

def remove_verification_files():
    """Remove those temporary files that have been created in the verification step"""
    # these occur during a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_TAPE:
            # get the directory that the temporary files are in
            batch_id = pr.migration.et_id
            # get the temporary directory
            verify_dir = os.path.join(settings.VERIFY_DIR, "batch{}".format(batch_id))
            # remove the directory
            if os.path.isdir(verify_dir):
                shutil.rmtree(verify_dir)
                logging.info("TIDY: deleting directory " + verify_dir)
            else:
                logging.error("TIDY: cannot find directory " + verify_dir)


def remove_file_list_digest():
    """Remove the file list and digest that were created in the PUT request"""
    # these occur during a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_TAPE:
            # get the file list and digest
            file_list_path = os.path.join(settings.FILE_LIST_PATH, "et_file_list_"+str(pr.pk) + ".txt")
            file_digest_path = os.path.join(settings.FILE_LIST_PATH, "et_file_digest_"+str(pr.pk) + ".txt")
            # delete if exist
            if os.path.exists(file_list_path):
                os.remove(file_list_path)
                logging.info("TIDY: deleting file " + file_list_path)
            else:
                logging.error("TIDY: cannot delete file " + file_list_path)

            if os.path.exists(file_digest_path):
                os.remove(file_digest_path)
                logging.info("TIDY: deleting file " + file_digest_path)
            else:
                logging.error("TIDY: cannot delete file " + file_digest_path)


def remove_original_files():
    """Remove the original files.  This is the whole point of the migration!"""
    # these occur during a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_TAPE:
            if os.path.isdir(pr.migration.original_path):
                shutil.rmtree(pr.migration.original_path)
                logging.info("TIDY: deleting directory " + pr.migration.original_path)
            else:
                logging.error("TIDY: cannot delete directory " + pr.migration.original_path)


def remove_put_requests():
    """Remove the put requests that are ON_TAPE"""
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # only do it if the files are on tape
        if pr.migration.stage == Migration.ON_TAPE:
            logging.info("TIDY: deleting PUT request {}".format(pr.pk))
            pr.delete()

def run():
    setup_logging(__name__)
    # these are individual loops to aid debugging and so we can turn them
    # on / off if we wish
    remove_verification_files()
    remove_file_list_digest()
    remove_original_files()
    remove_put_requests()
