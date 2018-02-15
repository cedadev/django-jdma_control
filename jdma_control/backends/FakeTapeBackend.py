"""Class for a JASMIN Data Migration App backend which emulates Elastic Tape."""

from jdma_control.backends.Backend import Backend

import os
from feedgen.feed import FeedGenerator
from datetime import datetime
import random
from shutil import copy, copy2

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.backends.ElasticTapeBackend import monitor_et_rss_feed
from jdma_control.backends import FakeTapeSettings as FS_Settings

def gen_test_feed():
    """Generate a test RSS(atom) feed."""
    fg = FeedGenerator()
    fg.title('JASMIN Elastic Tape Alerts')
    fg.link(href='http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php')
    fg.description('JASMIN Elastic Tape Alert feed</description')
    # we only care about the description tags
    # first transition the Migrations currently in PUTTING
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    # current alert number - iterate this
    c_alert = 0

    # for each PUT request get the Migration and determine if the type of the Migration is ON_DISK
    for pr in put_reqs:
        if pr.migration.stage == Migration.PUTTING:
            # get the et id and directory from it
            batch_id = pr.migration.external_id
            batch_dir = os.path.join(FS_Settings.FAKE_ET_DIR, "batch%04i" % batch_id)
            # check something has been written to the directory
            if len(os.listdir(batch_dir)) != 0:
                # write the completed description into the RSS feed
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                batch_desc = 'Batch {} successfully sent to storage at {}'.format(batch_id, current_time)
                batch_url = 'http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php#alert_id_{}'.format(c_alert)
                fe = fg.add_entry()
                fe.title('Batch {} successfully sent to storage'.format(batch_id))
                fe.id(batch_url)
                fe.link(href=batch_url, rel='alternate')
                fe.description(batch_desc)
                c_alert += 1
        elif pr.migration.stage == Migration.VERIFY_GETTING:
            batch_id = pr.migration.external_id
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # now output the retrieval mapping message
            fe = fg.add_entry()
            batch_url = 'http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php#alert_id_{}'.format(c_alert)
            fe.title('Retrieval request {} by {} for batch {}'.format(pr.pk, pr.user.name, batch_id))
            ret_map_desc = "Retrieval request {} by {} for batch {} at {}".format(pr.pk, pr.user.name, batch_id, current_time)
            fe.id(batch_url)
            fe.link(href=batch_url, rel='alternate')
            fe.description(ret_map_desc)
            c_alert += 1

            # output the retrieval completed message first
            fe = fg.add_entry()
            batch_url = 'http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php#alert_id_{}'.format(c_alert)
            fe.title('Retrieval request {} by {} completed.'.format(pr.pk, pr.user.name))
            ret_comp_desc = "Retrieval request {} by {} completed at {} with {}".format(pr.pk, pr.user.name, current_time, 0)
            fe.id(batch_url)
            fe.link(href=batch_url, rel='alternate')
            fe.description(ret_comp_desc)
            c_alert += 1

    # Generate the GET rss feed
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)

    for gr in get_reqs:
        if gr.stage == MigrationRequest.GETTING:
            # get the et id and current_time
            batch_id = gr.migration.external_id
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # now output the retrieval mapping message
            fe = fg.add_entry()
            batch_url = 'http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php#alert_id_{}'.format(c_alert)
            fe.title('Retrieval request {} by {} for batch {}'.format(gr.pk, gr.user.name, batch_id))
            ret_map_desc = "Retrieval request {} by {} for batch {} at {}".format(gr.pk, gr.user.name, batch_id, current_time)
            fe.id(batch_url)
            fe.link(href=batch_url, rel='alternate')
            fe.description(ret_map_desc)
            c_alert += 1
            # output the retrieval completed message first
            fe = fg.add_entry()
            batch_url = 'http://et-monitor.fds.rl.ac.uk/et_user/ET_AlertWatch.php#alert_id_{}'.format(c_alert)
            fe.title('Retrieval request {} by {} completed.'.format(gr.pk, gr.user.name))
            ret_comp_desc = "Retrieval request {} by {} completed at {} with {}".format(gr.pk, gr.user.name, current_time, 0)
            fe.id(batch_url)
            fe.link(href=batch_url, rel='alternate')
            fe.description(ret_comp_desc)
            c_alert += 1

    fg.rss_file(FS_Settings.ET_RSS_FILE)


def get_size_multiplier(multi_string):
    # get the multiplier from the string, e.g. MiB = 1024**2
    multi_maps = {"KiB":1024, "MiB":1024**2, "GiB":1024**3, "TiB":1024**4, "PiB":1024**5, "EiB":1024**6, "ZiB":1024**7, "YiB":1024**8,
                  "kB":1000,  "MB":1000**2,  "GB":1000**3,  "TB":1000**4,  "PB":1000**5,  "EB":1000**6,  "ZB":1000**7,  "YB":1000**8}
    return multi_maps[multi_string]


class FakeTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which emulates Elastic Tape.
       Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.VERIFY_DIR = FS_Settings.VERIFY_DIR

    def monitor(self):
        """Determine which batches have completed."""
        # generate the RSS feed
        gen_test_feed()
        # interpret the RSS feed
        completed_PUTs, completed_GETs = monitor_et_rss_feed(FS_Settings.ET_RSS_FILE)
        for cp in completed_PUTs:
            logging.info("Completed PUT: " + str(cp))
        for cg in completed_GETs:
            logging.info("Completed GET:" + str(cg))
        return completed_PUTs, completed_GETs

    def get(self, batch_id, user, workspace, target_dir):
        """Download a batch of files from the FakeTape to a target directory."""
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


    def put(self, filelist, user, workspace):
        """Put a list of files into the FakeTape - return the (randomly generated) external storage batch id"""
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

    def get_name():
        return "Fake Tape"

    def get_id():
        return "faketape"

    def user_has_put_permission(self, username, workspace):
        return Backend.user_has_put_permission(self, username, workspace)

    def user_has_get_permission(self, migration, username, workspace):
        return Backend.user_has_get_permission(self, migration, username, workspace)

    def user_has_put_quota(self, original_path, user, workspace):
        """Get the remaining quota for the user in the workspace"""
        return True

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the daemon processes can carry
           out the Migrations on behalf of the user."""
        return []
