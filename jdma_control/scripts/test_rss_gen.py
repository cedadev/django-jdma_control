"""Generate a test RSS feed so that it can be used in the TEST environment
   Output will go to
"""

import os
from feedgen.feed import FeedGenerator
from datetime import datetime

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest


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
            batch_id = pr.migration.et_id
            batch_dir = os.path.join(settings.FAKE_ET_DIR, "batch%04i" % batch_id)
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
            batch_id = pr.migration.et_id
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
            batch_id = gr.migration.et_id
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

    fg.rss_file(settings.ET_RSS_FILE)


def run():
    # generate the directory
    et_rss_dir = os.path.dirname(settings.ET_RSS_FILE)
    if not os.path.isdir(et_rss_dir):
        os.makedirs(et_rss_dir)
    gen_test_feed()
