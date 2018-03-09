"""Class for a JASMIN Data Migration App backend which targets Elastic Tape."""

from jdma_control.backends.Backend import Backend
from datetime import datetime
import feedparser
import os
import re
import subprocess

# Import elastic_tape client library after logging is set up
import elastic_tape.client
from elastic_tape.shared.error import StorageDError
import jdma_site.settings as settings
from jdma_control.backends import ElasticTapeSettings as ET_Settings

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


def monitor_et_rss_feed(feed):
    """Monitor the RSS feed from et and get the completed PUT requests and the
    completed GET requests"""
    et_rss = feedparser.parse(feed)
    completed_PUTs = []
    completed_GETs = []
    # need to do this backwards to get the mappings between the retrieval id and
    # batch id before the check for GET_COMPLETE is done
    for item in et_rss['items'][::-1]:
        if 'description' in item:
            rss_i = interpret_rss_status(item['description'])
            if rss_i.status == GET_COMPLETE:
                completed_GETs.append(rss_i)
            elif rss_i.status == PUT_COMPLETE:
                completed_PUTs.append(rss_i)
    return completed_PUTs, completed_GETs


class ElasticTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets Elastic Tape.
       Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.VERIFY_DIR = ET_Settings.VERIFY_DIR
        self.ARCHIVE_STAGING_DIR = ET_Settings.ARCHIVE_STAGING_DIR

    def monitor(self):
        """Determine which batches have completed."""
        completed_PUTs, completed_GETs = monitor_et_rss_feed(ET_Settings.ET_RSS_FILE)
        return completed_PUTs, completed_GETs

    def get(self, batch_id, user, workspace, target_dir, credentials):
        """Download a batch of files from the Elastic Tape to a target directory."""
        pass

    def put(self, filelist, user, workspace, credentials):
        """Put a list of files onto the Elastic Tape - return the external storage batch id"""
        # create connection to Elastic tape
        conn = elastic_tape.client.connect(ET_Settings.PUT_HOST, ET_Settings.PORT)
        # create a new batch for the user in the workspace
        batch = conn.newBatch(workspace, user)
        logging.info("Starting new batch for user: " + user)
        # do not allow overwriting of files or symbolic links
        batch.override = 0
        # override the requester to allow the JDMA server to handle the requests
        batch.requester = user
        # add each file in the filelist to the batch
        # each entry is a file, not a directory, as the os.walk of the directory returns just the files
        for fp in filelist:
            # extra check
            if not os.path.exists(fp):
                logging.debug("File not found: " + fp)
            else:
                # add the file
                batch.addFile(fp)
        # register the batch and get the id
        logging.info("Finished adding files, sending batch to ET server: "+ ET_Settings.PUT_HOST)
        batch_id = batch.register()

        logging.info("Batch with id: " + str(batch_id) + " created successfully")
        return int(batch_id)

    def user_has_put_permission(self, username, workspace, credentials):
        return Backend.user_has_put_permission(self, username, workspace, credentials)

    def user_has_get_permission(self, migration, username, workspace, credentials):
        return Backend.user_has_get_permission(self, migration, username, workspace, credentials)

    def user_has_put_quota(self, original_path, user, workspace, credentials):
        """Get the remaining quota for the user in the workspace"""
        return True
        # now get the size of the original_path directory
        # if pan_du exists then use it, otherwise use du
        if os.path.exists(ET_Settings.PAN_DU_EXE):
            # execute and interpret the PAN_DU_EXE command
            try:
                pan_du_output = subprocess.check_output([ET_Settings.PAN_DU_EXE, "-s", original_path])
                # get the path_size in bytes from the number in pos 4 and the multiplier in pos 5
                path_split = pan_du_output.split()
                path_size = int(path_split[4]) * get_size_multiplier(path_split[5])
            except:
                raise Exception("Error with pan_du command")

        else:
            # just run the normal du command
            #try:
            if True:
                du_output = subprocess.check_output(["/usr/bin/du", "-s", original_path])
                # get the path size from the first element of the split string
                path_size = int(du_output.split()[0])
            else:
            #except:
                raise Exception("Error with du command")

        return quota_remaining > path_size


    def get_name(self):
        return "Elastic Tape"


    def get_id(self):
        return "elastictape"


    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the daemon processes can carry
           out the Migrations on behalf of the user."""
        return []


    def minimum_object_size(self):
        """Minimum recommend size for elastic tape = 1GB? (check with Kev O'Neil)"""
        return 1*10**9
