"""Functions to verify files that have been migrated to tape.
   These files have been put on tape and then (temporarily) pulled back to
   disk before being verified by calculating the SHA256 digest and comparing it
   to the digest that was calculated (in jdma_transfer) before it was uploaded
   to tape.
   Running this will change the state of the migrations:
     VERIFYING->ON_TAPE
"""

import os
import subprocess
import logging
import hashlib

import jdma_control.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_lock import setup_logging


def calculate_digest(filename):
    # Calculate the hex digest of the file, using a buffer
    BUFFER_SIZE = 256 * 1024 # (256KB) - adjust this

    # create a sha256 object
    sha256 = hashlib.sha256()

    # read through the file
    with open(filename, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)
            if not data: # EOF
                break
            sha256.update(data)
    return "SHA256: {0}".format(sha256.hexdigest())


def read_digest_file(id):
    """Read the digest file for transfer request id"""
    digest_file = os.path.join(settings.FILE_LIST_PATH, "et_file_digest_"+str(id) + ".txt")
    # open file
    fh = open(digest_file, 'r')
    # get the lines
    lines = fh.readlines()
    digest = {}
    # each line contains: file_path, SHA256 digest
    for l in lines:
        # comma separated
        d = l.split(",")
        sha = d[1].strip("\n")[len(" SHA256: "):]
        digest[d[0]] = sha
    fh.close()
    return digest


def verify_files():
    """Verify the files that have been uploaded to tape and then downloaded
    back to a temporary directory."""
    # these are part of a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        # Check whether the request is in the verifying stage
        if pr.migration.stage == Migration.VERIFYING:
            # get the batch id
            batch_id = pr.migration.et_id
            # get the temporary directory
            verify_dir = os.path.join(settings.VERIFY_DIR, "batch{}".format(batch_id))
            # read the digest file
            digest = read_digest_file(pr.pk)

            # first check that each file exists in the temporary directory
            for k in digest:
                # filename is concatenation of verify_dir and the the original file path
                verify_file_path = verify_dir+k
                # check the file exists - if it doesn't then set the stage to FAILED
                # and write that the file couldn't be found in the failure_reason
                if not os.path.exists(verify_file_path):
                    pr.migration.stage = FAILED
                    pr.migration.failure_reason = "VERIFY: file " + verify_file_path + " could not be found."
                    logging.error("VERIFY: " + pr.migration.failure_reason)
                    pr.migration.save()
                    sys.exit(0)
                else:
                    # check that the digest matches
                    new_digest = calculate_digest(verify_file_path)[len(" SHA256:"):]
                    # get the actual file path to compare the digests
                    verify_base_path = os.path.join(settings.VERIFY_DIR, "batch{}".format(batch_id))
                    orig_file_path = verify_file_path.replace(verify_base_path, "")
                    # check that the digests match
                    if digest[orig_file_path] != new_digest:
                        # if not then indicate via the failure_reason
                        pr.migration.stage = FAILED
                        pr.migration.failure_reason = "VERIFY: file " + verify_file_path + " could not be found."
                        logging.error("VERIFY: " + pr.migration.failure_reason)
                        pr.migration.save()
                        sys.exit(0)
            # if we reach this part without exiting then the batch has verified successfully and we
            # can transition to ON_TAPE, ready for the tidy up process
            pr.migration.stage = Migration.ON_TAPE
            pr.migration.save()
            logging.info("Transition: batch ID: {} VERIFYING->ON_TAPE".format(pr.migration.et_id))

def run():
    setup_logging(__name__)
    verify_files()
