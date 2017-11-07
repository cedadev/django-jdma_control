"""Functions to transfer the files in a request to (PUT) / from (GET) the external storage.
   Running this will change the state of the migrations:
     PUT_PENDING->PUTTING
     GET_PENDING->GETTING
     VERIFYING->VERIFY_GETTING
   and will invoke Backend.Put and Backend.Get for PUT / GET operations to tape.
   The external_id of returned from Backend.Put will be recorded in the Migration object
"""

import os
import subprocess
import logging

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, MigrationFile
from jdma_control.scripts.jdma_lock import setup_logging
from jdma_control.scripts.jdma_verify import calculate_digest

import jdma_control.backends

def put_transfers():
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    # for each PUT request get the Migration and determine if the type of the Migration is PUT_PENDING
    for pr in put_reqs:
        # Check whether data is being put to tape
        if pr.migration.stage == Migration.PUT_PENDING:
            # create the file list of all the files (not directories) under the original directory
            # don't follow symbolic links!
            user_file_list = os.walk(pr.migration.original_path, followlinks=False)
            # copy full paths into a list
            filepaths = []
            for root, dirs, files in user_file_list:
                if len(files) != 0:
                    for fl in files:
                        # get the full path and append to the list
                        filepath = os.path.join(root, fl)
                        filepaths.append(filepath)

            # if there are not zero files in the filepaths list:
            if len(filepaths) != 0:
                # build the list of file paths and digests and store in the database
                for fp in filepaths:
                    # create a MigrationFile
                    mig_file = MigrationFile()
                    mig_file.path = fp
                    digest = calculate_digest(fp)
                    mig_file.digest = digest[len("SHA256: "):]
                    mig_file.migration = pr.migration
                    mig_file.save()

                # Use the Backend stored in settings.JDMA_BACKEND_OBJECT to do the put
                external_id = int(settings.JDMA_BACKEND_OBJECT.put(filepaths))
                pr.migration.external_id = external_id
                pr.migration.stage = Migration.PUTTING
                pr.migration.save()
                logging.info("Transition: batch ID: {} PUT_PENDING->PUTTING".format(pr.migration.external_id))

        # check if data is now on tape and should be pulled back for verification
        elif pr.migration.stage == Migration.VERIFY_PENDING:
            # get the batch id
            batch_id = pr.migration.pk
            external_id = pr.migration.external_id
            workspace = pr.migration.workspace
            # create the target directory
            target_dir = os.path.join(settings.JDMA_BACKEND_OBJECT.VERIFY_DIR, "batch{}".format(pr.migration.external_id))
            if not os.path.isdir(target_dir):
                os.makedirs(target_dir)
            # use Backend.get to pull back the files to a temporary directory
            settings.JDMA_BACKEND_OBJECT.get(external_id, target_dir)
            pr.migration.stage = Migration.VERIFY_GETTING
            pr.migration.save()
            logging.info("Transition: batch ID: {} VERIFY_PENDING->VERIFY_GETTING".format(pr.migration.external_id))


def get_transfers():
    # now loop over the GET requests
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    # for each GET request get the Migration and determine if the type of the Migration is GET_PENDING
    for gr in get_reqs:
        # Check whether data is to be got from the tape
        if gr.stage == MigrationRequest.GET_PENDING:
            external_id = gr.migration.external_id
            workspace = gr.migration.workspace
            target_dir = gr.target_path
            # check whether the target directory is the same as the original
            # Migration directory.  If it is then modify target_dir to be "/"
            if target_dir == gr.migration.original_path:
                target_dir = "/"
            # use the backend to pull back the files to a temporary directory
            settings.JDMA_BACKEND_OBJECT.get(external_id, target_dir)
            gr.stage = MigrationRequest.GETTING
            gr.save()
            logging.info("Transition: request ID: {} GET_PENDING->GETTING".format(gr.pk))

def run():
    # setup the logging
    setup_logging(__name__)
    put_transfers()
    get_transfers()
