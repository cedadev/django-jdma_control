"""Functions to monitor the files in a request to (PUT) / from (GET) external
   storage, using the backend monitor function Backend.Monitor for the backend
   in the request.

   Running this will change the state of the migrations:
     PUTTING->VERIFY_PENDING
     VERIFYING->ON_STORAGE
     GETTING->ON_DISK
"""

import logging

from django.db.models import Q

import signal
import sys
from time import sleep
import random

import jdma_control.backends
import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.backends.ConnectionPool import ConnectionPool
from jdma_control.scripts.common import split_args
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level

connection_pool = ConnectionPool()

def monitor_put(completed_PUTs, backend_object):
    """Monitor the PUTs and MIGRATES and transition from PUTTING to
    VERIFY_PENDING (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # now loop over the PUT requests
    pr_objs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(locked=False)
        & Q(stage=MigrationRequest.PUTTING)
        & Q(migration__stage=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )

    for pr in pr_objs:
        # This is the standard locking code.  See functions in "jdma_lock" for full
        # details
        if not pr:
            return
        if not pr.lock():
            return
        ###

        # check whether it's in the completed_PUTs
        if pr.migration.external_id in completed_PUTs:
            # if it is then migrate to VERIFY_PENDING
            pr.stage = MigrationRequest.VERIFY_PENDING
            # reset the last_archive - needed for verify_get
            pr.last_archive = 0
            pr.save()
            logging.info((
                "Transition: request ID: {} external ID {} PUTTING->VERIFY_PENDING"
            ).format(pr.pk, pr.migration.external_id))
        pr.unlock()


def monitor_get(completed_GETs, backend_object):
    """Monitor the GETs and transition from GETTING to ON_DISK (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    gr_objs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(locked=False)
        & Q(stage=MigrationRequest.GETTING)
        & Q(migration__storage__storage=storage_id)
    )

    for gr in gr_objs:
        # This is the standard locking code.  See functions in "jdma_lock" for full
        # details
        if not gr:
            return
        if not gr.lock():
            return
        ###

        if gr.transfer_id in completed_GETs:
            # There may be multiple completed_GETs with external_id as Migrations
            # can be downloaded by multiple MigrationRequests
            # The only way to check is to make sure all the files in the
            # original migration are present in the target_dir
            gr.stage = MigrationRequest.GET_UNPACKING
            # reset the last archive counter
            gr.last_archive = 0
            gr.save()
            logging.info((
                "Transition: request ID: {} GETTING->GET_UNPACKING"
            ).format(gr.pk))
        gr.unlock()


# verify is now handled by quick_verify

def monitor_delete(completed_DELETEs, backend_object):
    """Monitor the DELETEs and transition from DELETING to DELETE_TIDY"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    dr_objs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        # & Q(locked=False)
        & Q(stage=MigrationRequest.DELETING)
        & Q(migration__storage__storage=storage_id)
    )

    for dr in dr_objs:
        # This is the standard locking code.  See functions in "jdma_lock" for full
        # details
        if not dr:
            return
        if not dr.lock():
            return
        ###

        if dr.migration.external_id in completed_DELETEs:
            dr.stage = MigrationRequest.DELETE_TIDY
            logging.info((
                "Transition: request ID: {} external ID: {} DELETING->DELETE_TIDY"
            ).format(dr.pk, dr.migration.external_id))
            # reset the last archive counter
            dr.last_archive = 0
            dr.save()
        dr.unlock()


def process(backend):
    backend_object = backend()
    completed_PUTs, completed_GETs, completed_DELETEs = backend_object.monitor()
    # monitor the puts and the gets
    monitor_put(completed_PUTs, backend_object)
    monitor_get(completed_GETs, backend_object)
    monitor_delete(completed_DELETEs, backend_object)


def exit_handler(signal, frame):
    logging.info("Stopping jdma_monitor")
    sys.exit(0)


def run_loop(backend):
    try:
        if backend is None:
            for backend in jdma_control.backends.get_backends():
                process(backend)
        else:
            if not backend in jdma_control.backends.get_backend_ids():
                logging.error("Backend: " + backend + " not recognised.")
            else:
                backend = jdma_control.backends.get_backend_from_id(backend)
                process(backend)
    except (KeyboardInterrupt, SystemExit):
        connection_pool.close_all_connections()
        sys.exit(0)


def run(*args):
    global connection_pool

    config = read_process_config("jdma_monitor")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )

    logging.info("Starting jdma_monitor")

    # monitor the backends for completed GETs and PUTs (to et)
    # have to monitor each backend
    # setup exit signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # process the arguments
    arg_dict = split_args(args)
    if "backend" in arg_dict:
        backend = arg_dict["backend"]
    else:
        backend = None

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
            run_loop(backend)
            sleep(5 + random.random())
    else:
        run_loop(backend)
