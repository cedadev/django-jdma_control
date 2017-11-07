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
            batch_dir = os.path.join(settings.JDMA_BACKEND_OBJECT.FAKE_ET_DIR, "batch%04i" % batch_id)
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

    fg.rss_file(settings.JDMA_BACKEND_OBJECT.ET_RSS_FILE)


class FakeTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which emulates Elastic Tape.
       Inherits from Backend class and overloads inherited functions."""
    ET_RSS_FILE = "/jdma_rss_feed/test_feed.xml"
    FAKE_ET_DIR = "/home/vagrant/fake_et"
    VERIFY_DIR  = "/home/vagrant/verify_dir"

    def monitor(self):
        """Determine which batches have completed."""
        # generate the RSS feed
        gen_test_feed()
        completed_PUTs, completed_GETs = monitor_et_rss_feed(settings.JDMA_BACKEND_OBJECT.ET_RSS_FILE)
        return completed_PUTs, completed_GETs

    def get(self, batch_id, target_dir):
        """Download a batch of files from the FakeTape to a target directory."""
        # download a batch from the fake_et
        # get the directory which has the batch in it
        batch_dir = os.path.join(FakeTapeBackend.FAKE_ET_DIR, "batch%04i" % batch_id)
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


    def put(self, filelist):
        """Put a list of files into the FakeTape - return the (randomly generated) external storage batch id"""
        # create a batchid as a random number between 0 and 9999
        batch_id = random.uniform(0,9999)
        # create the directory to store the batch in
        batch_dir = os.path.join(FakeTapeBackend.FAKE_ET_DIR, "batch%04i" % batch_id)
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
        return batch_id

    def get_name(self):
        return "Fake Tape."

    def user_has_permission(self, user, workspace):
        return True

    def user_has_remaining_quota(self, filelist, user, workspace):
        return True
