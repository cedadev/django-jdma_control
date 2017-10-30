"""Functions to transfer the files in a request to (PUT) / from (GET) elastic tape,
   using et_put and et_get.
   Running this will change the state of the migrations:
     PUT_PENDING->PUTTING
     GET_PENDING->GETTING
     VERIFYING->VERIFY_GETTING
   and will invoke et_put and et_get for PUT / GET operations to tape.
   The batch id of et_put will be recorded in the Migration object
"""

import os
import subprocess
import logging

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.scripts.jdma_lock import setup_logging
from jdma_control.scripts.jdma_verify import calculate_digest

def put_transfers():
    # first check the file list directory exists
    if not os.path.isdir(settings.FILE_LIST_PATH):
        os.makedirs(settings.FILE_LIST_PATH)

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
                # write to the jdma_file_list
                file_list_path = os.path.join(settings.FILE_LIST_PATH, "et_file_list_"+str(pr.pk) + ".txt")
                fh_list = open(file_list_path, 'w')
                file_digest_path = os.path.join(settings.FILE_LIST_PATH, "et_file_digest_"+str(pr.pk) + ".txt")
                fh_digest = open(file_digest_path, 'w')
                # write the filepath and the digest (SHA-256)
                for fp in filepaths:
                    # the list for the et_put tool
                    fh_list.write(fp)
                    fh_list.write("\n")

                    # the digest
                    fh_digest.write(fp)
                    sha256 = calculate_digest(fp)
                    fh_digest.write(", "+sha256)
                    fh_digest.write("\n")
                fh_list.close()
                fh_digest.close()
                # get the exe path
                if settings.TESTING:
                    et_put_exe_path = "/Coding/django-jdma_control/jdma_control/bin/et_put_emulator.py"
                else:
                    et_put_exe_path = "/usr/bin/et_put.py"
                # spawn the process and wait for the output
                output = subprocess.check_output([et_put_exe_path, "-f", file_list_path, "-w", pr.migration.workspace])
                # get the batch id - string returned is "Batch ID: xxxx"
                et_id = int(output[len("Batch ID: "):])
                pr.migration.et_id = et_id
                pr.migration.stage = Migration.PUTTING
                pr.migration.save()
                logging.info("Transition: batch ID: {} PUT_PENDING->PUTTING".format(pr.migration.et_id))

        # check if data is now on tape and should be pulled back for verification
        elif pr.migration.stage == Migration.VERIFY_PENDING:
            # get the batch id
            batch_id = pr.migration.et_id
            workspace = pr.migration.workspace
            # get the exe path
            if settings.TESTING:
                et_get_exe_path = "/Coding/django-jdma_control/jdma_control/bin/et_get_emulator.py"
            else:
                et_get_exe_path = "/usr/bin/et_get.py"
            # create the target directory
            target_dir = os.path.join(settings.VERIFY_DIR, "batch{}".format(batch_id))
            if not os.path.isdir(target_dir):
                os.makedirs(target_dir)
            # use et_get to pull back the files to a temporary directory
            output = subprocess.check_output([et_get_exe_path, "-b", str(batch_id), "-r", target_dir, "-w", workspace])
            pr.migration.stage = Migration.VERIFY_GETTING
            pr.migration.save()
            logging.info("Transition: batch ID: {} VERIFY_PENDING->VERIFY_GETTING".format(pr.migration.et_id))


def get_transfers():
    # get the exe path
    if settings.TESTING:
        et_get_exe_path = "/Coding/django-jdma_control/jdma_control/bin/et_get_emulator.py"
    else:
        et_get_exe_path = "/usr/bin/et_get.py"
    # now loop over the PUT requests
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    # for each GET request get the Migration and determine if the type of the Migration is GET_PENDING
    for gr in get_reqs:
        # Check whether data is to be got from the tape
        if gr.stage == MigrationRequest.GET_PENDING:
            batch_id = gr.migration.et_id
            workspace = gr.migration.workspace
            target_dir = gr.target_path
            # check whether the target directory is the same as the original
            # Migration directory.  If it is then modify target_dir to be "/"
            if target_dir == gr.migration.original_path:
                target_dir = "/"
            # use et_get to pull back the files to a temporary directory
            # this runs et_get in the background
            output = subprocess.Popen(["nohup", et_get_exe_path, "-b", str(batch_id), "-r", target_dir, "-w", workspace],
                                      stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'a'),
                                      preexec_fn=os.setpgrp)
            gr.stage = MigrationRequest.GETTING
            gr.save()
            logging.info("Transition: request ID: {} GET_PENDING->GETTING".format(gr.pk))

def run():
    # setup the logging
    setup_logging(__name__)
    put_transfers()
    get_transfers()
