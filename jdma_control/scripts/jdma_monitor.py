"""Functions to monitor the files in a request to (PUT) / from (GET) elastic tape,
   using et_put and et_get.
   The RSS feed will be queried / parsed and action taking when PUTs and GETS
   have completed.
   Running this will change the state of the migrations:
     PUTTING->VERIFY_PENDING
     VERIFYING->ON_TAPE
     GETTING->ON_DISK
"""

import feedparser
import os
import re
import logging
import subprocess
from datetime import datetime

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.scripts.jdma_lock import setup_logging

from jasmin_ldap.core import *
from jasmin_ldap.query import *

# statuses for the RSS items
PUT_COMPLETE = 0
GET_COMPLETE = 1
UNKNOWN_STATUS = -1

# mapping between retrieval id and et batch id
retrieval_batch_map = {}

class rss_item_status:
    """Class / struct to hold the status of an individual RSS item"""
    def __init__(self):
        self.status = UNKNOWN_STATUS        # status
        self.date = None                    # date of completion
        self.id = -1                        # batch id of et request

    def __eq__(self, rhs):
        return self.id == rhs

    def __str__(self):
        return str(self.status) + " " + str(self.date) + " " + str(self.id)


def interpret_rss_status(item_desc):
    """Parse an individual RSS item's description and determine the status and info:
       1. PUT_COMPLETE or GET_COMPLETE
       2. The batch id
       3. The date the request completed

       item_desc: string
       returns: rss_item_status
    """
    rss_i = rss_item_status()
    # regexs for get and put and mapping the retrieval id to the batch id
    put_rx = r"Batch ([0-9]+) successfully sent to storage at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    get_rx = r"Retrieval request ([0-9]+) by ([a-zA-Z0-9_-]+) completed at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) with \d"
    map_rx = r"Retrieval request ([0-9]+) by ([a-zA-Z0-9_-]+) for batch ([0-9]+) at (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"

    # match each against the item
    put_match = re.match(put_rx, item_desc)
    get_match = re.match(get_rx, item_desc)
    map_match = re.match(map_rx, item_desc)


    # check which one matched and process
    if put_match is not None:
        rss_i.status = PUT_COMPLETE
        rss_i.id = int(put_match.group(1))
        rss_i.date = datetime.strptime(put_match.group(2), "%Y-%m-%d %H:%M:%S")
    elif get_match is not None:
        # need to map the retrieval id to the batch id using retrieval_batch_map before this stage
        ret_id = get_match.group(1)
        if ret_id in retrieval_batch_map:
            rss_i.status = GET_COMPLETE
            rss_i.id = int(retrieval_batch_map[ret_id])
            rss_i.date = datetime.strptime(get_match.group(3), "%Y-%m-%d %H:%M:%S")
    elif map_match is not None:
        # build the mapping from the retrieval id to the et batch id
        rss_i.status = UNKNOWN_STATUS
        retrieval_batch_map[map_match.group(1)] = map_match.group(3)
    return rss_i


def monitor_et_rss_feed():
    """Monitor the RSS feed from et and get the completed PUT requests and the
    completed GET requests"""
    # open and parse the RSS feed
    if settings.TESTING:
        feed = settings.ET_RSS_FILE
    else:
        feed = settings.ET_RSS_URL
    et_rss = feedparser.parse(feed)
    completed_PUTs = []
    completed_GETs = []
    # need to do this backwards to get the mappings between the retrieval id and
    # batch id before the check for GET_COMPLETE is done
    for item in et_rss['items'][::-1]:
        rss_i = interpret_rss_status(item['description'])
        if rss_i.status == GET_COMPLETE:
            completed_GETs.append(rss_i)
        elif rss_i.status == PUT_COMPLETE:
            completed_PUTs.append(rss_i)
    return completed_PUTs, completed_GETs


def monitor_put(completed_PUTs):
    """Monitor the PUTs and transition from PUTTING to VERIFY_PENDING (or FAILED)"""
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        if pr.migration.stage == Migration.PUTTING:
            # check whether it's in the completed_PUTs
            if pr.migration.et_id in completed_PUTs:
                # if it is then migrate to VERIFY_PENDING
                pr.migration.stage = Migration.VERIFY_PENDING
                pr.migration.save()
                logging.info("Transition: batch ID: {} PUTTING->VERIFY_PENDING".format(pr.migration.et_id))


def monitor_get(completed_GETs):
    """Monitor the GETs and transition from GETTING to ON_DISK (or FAILED)"""
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    # get the ldap servers
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

    for gr in get_reqs:
        if gr.stage == MigrationRequest.GETTING:
            if gr.migration.et_id in completed_GETs:
                # There may be multiple completed_GETs with et_id as Migrations
                # can be downloaded by multiple MigrationRequests
                # The only way to check is to make sure all the files in the
                # original migration are present in the target_dir
                # change the owner, group and permissions of the file to match that of the original
                # form the user query
                with Connection.create(ldap_servers) as conn:
                    # query for the user
                    query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_USER).filter(uid=gr.migration.unix_user_id)
                    # check for a valid return
                    if len(query) == 0:
                        logging.error("Unix user id: {} not found from LDAP in monitor_get".format(gr.migration.unix_user_id))
                        raise Exception
                    # use just the first returned result
                    q = query[0]
                    # # check that the keys exist in q
                    if not ("uidNumber" in q):
                        logging.error("uidNumber not in returned LDAP query for user id {}".format(gr.migration.unix_user_id))
                        raise Exception
                    else:
                        uidNumber = q["uidNumber"][0]

                    # query for the group
                    query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn=gr.migration.unix_group_id)
                    # check for a valid return
                    if len(query) == 0:
                        logging.error("Unix group id: {} not found from LDAP in monitor_get".format(gr.migration.unix_group_id))
                        raise Exception
                    # use just the first returned result
                    q = query[0]
                    # check that the keys exist in q
                    if not ("gidNumber" in q):
                        logging.error("gidNumber not in returned LDAP query for group id {}".format(gr.migration.unix_group_id))
                        raise Exception
                    else:
                        gidNumber = q["gidNumber"][0]

                    # change the directory owner / group
                    subprocess.call(["/usr/bin/sudo", "/bin/chown", "-R", str(uidNumber)+":"+str(gidNumber), gr.target_path])

                gr.stage = MigrationRequest.ON_DISK
                gr.save()
                logging.info("Transition: request ID: {} GETTING->ON_DISK".format(gr.pk))


def monitor_verify(completed_GETs):
    """Monitor the VERIFYs and transition from VERIFY_GETTING to VERIFYING"""
    verify_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for vr in verify_reqs:
        if vr.migration.stage == Migration.VERIFY_GETTING:
            # This is fine (in contrast) to above monitor_get as
            # 1. There is only one GET for each et_id in the VERIFY stage
            # 2. GETs (for none-VERIFY stage, i.e. to actaully download the data)
            # cannot be issued until the Migration.status is ON_TAPE
            if vr.migration.et_id in completed_GETs:
                vr.migration.stage = Migration.VERIFYING
                vr.migration.save()
                logging.info("Transition: batch ID: {} VERIFY_GETTING->VERIFYING".format(vr.migration.et_id))


def run():
    setup_logging(__name__)
    # monitor the rss feed for completed GETs and PUTs (to et)
    completed_PUTs, completed_GETs = monitor_et_rss_feed()
    # monitor the puts and the gets
    monitor_put(completed_PUTs)
    monitor_get(completed_GETs)
    monitor_verify(completed_GETs)
