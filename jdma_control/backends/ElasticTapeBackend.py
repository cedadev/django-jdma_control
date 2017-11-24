"""Class for a JASMIN Data Migration App backend which targets Elastic Tape."""

from jdma_control.backends.Backend import Backend
from datetime import datetime
import feedparser
import os
import re

# Import elastic_tape client library after logging is set up
import elastic_tape.client
from elastic_tape.shared.error import StorageDError

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
        rss_i = interpret_rss_status(item['description'])
        if rss_i.status == GET_COMPLETE:
            completed_GETs.append(rss_i)
        elif rss_i.status == PUT_COMPLETE:
            completed_PUTs.append(rss_i)
    return completed_PUTs, completed_GETs


class ElasticTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets Elastic Tape.
       Inherits from Backend class and overloads inherited functions."""

    def monitor(self):
        """Determine which batches have completed."""
        completed_PUTs, completed_GETs = monitor_et_rss_feed(settings.JDMA_BACKEND_OBJECT.ET_RSS_FILE)
        return completed_PUTs, completed_GETs

    def get(self, batch_id, target_dir):
        """Download a batch of files from the Elastic Tape to a target directory."""
        pass

    def put(self, filelist):
        """Put a list of files onto the Elastic Tape - return the (randomly generated) external storage batch id"""

    def get_name(self):
        return "Elastic Tape"
