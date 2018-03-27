"""Functions to monitor the files in a request to (PUT) / from (GET) external storage,
   using the backend monitor function Backend.Monitor for the backend in the request.
   A notification email will be sent on GET / PUT completion.

   Running this will change the state of the migrations:
     PUTTING->VERIFY_PENDING
     VERIFYING->ON_STORAGE
     GETTING->ON_DISK
"""

import logging

from django.db.models import Q

import jdma_control.backends
import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging


def monitor_put(completed_PUTs, backend_object):
    """Monitor the PUTs and MIGRATES and transition from PUTTING to
    VERIFY_PENDING (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.PUTTING)
        & Q(migration__stage=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # check whether locked
        if pr.locked:
            continue
        # check whether it's in the completed_PUTs
        if pr.migration.external_id in completed_PUTs:
            # lock the migration
            pr.lock()
            # if it is then migrate to VERIFY_PENDING
            pr.stage = MigrationRequest.VERIFY_PENDING
            # reset the last_archive - needed for verify_get
            pr.last_archive = 0
            pr.locked = False
            pr.save()
            logging.info((
                "Transition: batch ID: {} PUTTING->VERIFY_PENDING"
            ).format(pr.migration.external_id))


def monitor_get(completed_GETs, backend_object):
    """Monitor the GETs and transition from GETTING to ON_DISK (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(stage=MigrationRequest.GETTING)
        & Q(migration__storage__storage=storage_id)
    )
    for gr in get_reqs:
        if gr.migration.external_id in completed_GETs:
            if gr.locked:
                continue
            gr.lock()
            # There may be multiple completed_GETs with external_id as Migrations
            # can be downloaded by multiple MigrationRequests
            # The only way to check is to make sure all the files in the
            # original migration are present in the target_dir
            gr.stage = MigrationRequest.GET_UNPACKING
            # reset the last archive counter
            gr.last_archive = 0
            gr.locked = False
            gr.save()
            logging.info((
                "Transition: request ID: {} GETTING->ON_DISK"
            ).format(gr.pk))


def monitor_verify(completed_GETs, backend_object):
    """Monitor the VERIFYs and transition from VERIFY_GETTING to VERIFYING"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    verify_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.VERIFY_GETTING)
        & Q(migration__storage__storage=storage_id)
    )

    for vr in verify_reqs:
        if vr.locked:
            continue
        if vr.migration.external_id in completed_GETs:
            vr.lock()
            vr.stage = MigrationRequest.VERIFYING
            logging.info((
                "Transition: batch ID: {} VERIFY_GETTING->VERIFYING"
            ).format(vr.migration.external_id))
            # reset the last archive counter
            vr.last_archive = 0
            vr.locked = False
            vr.save()


def monitor_delete(completed_DELETEs, backend_object):
    """Monitor the DELETEs and transition from DELETING to DELETE_TIDY"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    delete_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(stage=MigrationRequest.DELETING)
        & Q(migration__storage__storage=storage_id)
    )

    for dr in delete_reqs:
        if dr.locked:
            continue
        if dr.migration.external_id in completed_DELETEs:
            dr.lock()
            dr.stage = MigrationRequest.DELETE_TIDY
            logging.info((
                "Transition: batch ID: {} DELETING->DELETE_TIDY"
            ).format(dr.migration.external_id))
            # reset the last archive counter
            dr.last_archive = 0
            dr.locked = False
            dr.save()


def process(backend):
    backend_object = backend()
    completed_PUTs, completed_GETs, completed_DELETEs = backend_object.monitor()
    # monitor the puts and the gets
    monitor_put(completed_PUTs, backend_object)
    monitor_get(completed_GETs, backend_object)
    monitor_verify(completed_GETs, backend_object)
    monitor_delete(completed_DELETEs, backend_object)


def run(*args):
    # monitor the backends for completed GETs and PUTs (to et)
    # have to monitor each backend
    setup_logging(__name__)
    if len(args) == 0:
        for backend in jdma_control.backends.get_backends():
            process(backend)
    else:
        backend = args[0]
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend)
