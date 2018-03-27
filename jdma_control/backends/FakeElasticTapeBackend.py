"""Class for a JASMIN Data Migration App backend which emulates Elastic Tape."""

from jdma_control.backends.Backend import Backend

import os
import random
from shutil import copy, copy2
import logging

from jdma_control.backends.ElasticTapeBackend import monitor_et_rss_feed
from jdma_control.backends import FakeElasticTapeSettings as FS_Settings

class FakeElasticTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which emulates Elastic Tape.
       Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.VERIFY_DIR = FS_Settings.VERIFY_DIR
        self.ARCHIVE_STAGING_DIR = FS_Settings.ARCHIVE_STAGING_DIR

    def monitor(self):
        """Determine which batches have completed."""
        # imported here to avoid circular dependacies
        # from jdma_control.backends.FakeElasticTapeTools import gen_test_feed
        # # generate the RSS feed
        # gen_test_feed()
        # # interpret the RSS feed
        # completed_PUTs, completed_GETs = monitor_et_rss_feed(FS_Settings.ET_RSS_FILE)
        # for cp in completed_PUTs:
        #     logging.info("Completed PUT: " + str(cp))
        # for cg in completed_GETs:
        #     logging.info("Completed GET:" + str(cg))
        # return completed_PUTs, completed_GETs
        return [],[],[]

    def get(self, batch_id, user, workspace, target_dir, credentials):
        """Download a batch of files from the FakeElasticTape to a target directory."""
        # download a batch from the fake_et
        # get the directory which has the batch in it
        batch_dir = os.path.join(FS_Settings.FAKE_ET_DIR, "batch%04i" % batch_id)
        # walk the directory and copy
        user_file_list = os.walk(batch_dir, followlinks=False)
        # copy full paths into a list
        filepaths = []
        for root, dirs, files in user_file_list:
            if len(files) != 0:
                for fl in files:
                    # get the full path and append to the list
                    filepath = os.path.join(root, fl)
                    filepaths.append(filepath)
        # copy the files to the verify directory
        for fp in filepaths:
            # strip the batch_dir from the fp
            target_fp = target_dir + fp.replace(batch_dir, "")
            # get just the path
            target_path = os.path.dirname(target_fp)
            if not os.path.isdir(target_path):
                os.makedirs(target_path)
            copy(fp, target_fp)
            logging.info("GET: " + fp + " to: " + target_fp)


    def put(self, filelist, user, workspace, credentials):
        """Put a list of files into the FakeElasticTape - return the (randomly generated) external storage batch id"""
        # create a batchid as a random number between 0 and 9999
        batch_id = random.uniform(0,9999)
        # create the directory to store the batch in
        batch_dir = os.path.join(FS_Settings.FAKE_ET_DIR, "batch%04i" % batch_id)
        if not os.path.isdir(batch_dir):
            os.makedirs(batch_dir)
        # now copy the files
        for fname in filelist:
            # trim any trailing slash
            if fname[0] == "/":
                dest_fname = fname[1:]
            else:
                dest_fname = fname
            # create the destination filename
            dest_file = os.path.join(batch_dir, dest_fname)
            dest_dir = os.path.dirname(dest_file)
            # create the destination directory if it doesn't exist
            if not os.path.isdir(dest_dir):
                os.makedirs(dest_dir)
            # do the copy
            copy2(fname, dest_file)
            logging.info("PUT: " + fname + " to: " + dest_file)
        return batch_id

    def get_name(self):
        return "Fake Elastic Tape"

    def get_id(self):
        return "fakeelastictape"

    def user_has_put_permission(self, username, workspace):
        return Backend.user_has_put_permission(self, username, workspace)

    def user_has_get_permission(self, conn):
        return Backend.user_has_get_permission(self, migration, username, workspace)

    def user_has_put_quota(self, original_path, user, workspace, credentials):
        """Get the remaining quota for the user in the workspace"""
        return True

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the daemon processes can carry
           out the Migrations on behalf of the user."""
        return []

    def minimum_object_size(self):
        """Emulating tape, so make size smaller to make it easy to test = 1MB"""
        return 1*10**6
