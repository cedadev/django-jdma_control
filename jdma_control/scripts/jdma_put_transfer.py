import logging
import sys
import signal
from time import sleep
import random

from jdma_transfer import put_transfers

import jdma_site.settings as settings
import jdma_control.backends.AES_tools as AES_tools
import jdma_control.backends
from jdma_control.scripts.common import split_args

from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level

def process(backend_object, key):
    """Run the put_transfer processes on a backend."""
    put_transfers(backend_object, key)

def shutdown_handler(signum, frame):
    logging.info("Stopping jdma_transfer")
    sys.exit(0)

def run_loop(backend_objects):
    # Run the main loop over and over
    try:
        # read the decrypt key
        key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)
        for backend_object in backend_objects:
            process(backend_object, key)
    except SystemExit:
        for backend_object in backend_objects:
            backend_object.exit()
        sys.exit(0)
    except Exception as e:
        # catch all exceptions as we want this to run in a loop for all
        # backends and transfers - we don't want one transfer to crash out
        # the transfer daemon with a single bad transfer!
        # output the exception to the log so we can see what went wrong
        logging.error(str(e))

def run(*args):
    # setup the logging
    # setup exit signal handling
    global connection_pool
    config = read_process_config("jdma_transfer")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_transfer")

    # remap signals to shutdown handler which in turn calls sys.exit(0)
    # and raises SystemExit exception
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # process the arguments
    arg_dict = split_args(args)

    # create a list of backend objects to run process on
    # are we running one backend or many?
    backend_objects = []
    if "backend" in arg_dict:
        backend = arg_dict["backend"]
        # one backend
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend_class = jdma_control.backends.get_backend_from_id(backend)
            backend_objects.append(backend_class())
    else:
        # all the backends
        for backend in jdma_control.backends.get_backends():
            backend_objects.append(backend())

    # decide whether to run as a daemon
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
            run_loop(backend_objects)
            # add a random amount of time to prevent(?) race conditions
            sleep(5 + random.random())
    else:
        run_loop(backend_objects)
