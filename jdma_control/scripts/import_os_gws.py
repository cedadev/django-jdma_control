"""Create the object store groupworkspace names, gws manager, quota and email
   from the elastic tape quotas and create user records, groupworkspace records
   and object store quotas.
   This script is run via ./manage runscript"""

import jdma_site.settings as settings
import logging
import sys
import signal
from time import sleep
from jdma_control.models import User, Groupworkspace, StorageQuota
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level
from jdma_control.scripts.common import split_args
from jdma_control.scripts.import_et_gws import get_et_gws_from_url
from jdma_control.scripts.import_et_gws import create_user_entry, create_quota_entry

def create_user_gws_quotas(data):
    # Create the User, GroupWorkspace and StorageQuota from each line of the
    # data
    storageid = StorageQuota.get_storage_index("objectstore")
    for line in data:
        if len(line) == 4:
            # create user entry
            new_gws = create_user_entry(line)
            # create the new storage quota
            create_quota_entry(storageid, new_gws, 32 * 10**12, 0)

def exit_handler(signal, frame):
    logging.info("Stopping import_os_gws")
    sys.exit(0)

def run(*args):
    # setup the logging
    config = read_process_config("import_os_gws")
    logging.basicConfig(
        format=get_logging_format(),
        level="INFO",
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting import_os_gws")

    # setup exit signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    data = get_et_gws_from_url(config["OS_EXPORT_URL"])

    # decide whether to run as a daemon
    arg_dict = split_args(args)
    if "daemon" in arg_dict:
        if arg_dict["daemon"].lower() == "true":
            daemon = True
        else:
            daemon = False
    else:
        daemon = False

    # run as a daemon or one shot
    if daemon:
        # loop this indefinitely until the exit signals are triggered
        while True:
            create_user_gws_quotas(data)
            sleep(int(config["RUN_EVERY"]))
    else:
        create_user_gws_quotas(data)
