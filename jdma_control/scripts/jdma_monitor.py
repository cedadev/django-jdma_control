"""Functions to monitor the files in a request to (PUT) / from (GET) external storage,
   using the backend monitor function Backend.Monitor for the backend in the request.
   A notification email will be sent on GET / PUT completion.

   Running this will change the state of the migrations:
     PUTTING->VERIFY_PENDING
     VERIFYING->ON_STORAGE
     GETTING->ON_DISK
"""

import logging
import subprocess

from django.core.mail import send_mail
from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.scripts.jdma_lock import setup_logging
from jdma_control.scripts.jdma_verify import get_permissions_string

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_control.backends

def send_get_notification_email(get_req, backend_object):
    """Send an email to the user to notify them that their batch upload has been completed
     var jdma_control.models.User user: user to send notification email to
    """
    user = get_req.user

    if not user.notify:
        return

    # to address is notify_on_first
    toaddrs = [user.email]
    # from address is just a dummy address
    fromaddr = "support@ceda.ac.uk"

    # subject
    subject = (
        "[JDMA] - Notification of batch download from {}"
    ).format(backend_object.get_name())

    msg = (
        "GET request has succesfully completed downloading from external "
        "storage: {}\n"
    ).format(backend_object.get_name())
    msg += (
        "    Request id\t\t: {}\n"
    ).format(str(get_req.pk))
    msg += (
        "    Stage\t\t\t: {}\n"
    ).format(MigrationRequest.REQ_STAGE_LIST[get_req.stage])
    msg += (
        "    Date\t\t\t: {}\n"
    ).format(get_req.date.isoformat()[0:16].replace("T"," "))
    msg += (
        "    Target path\t\t: {}\n"
    ).format(get_req.target_path)
    msg += "\n"
    msg += "------------------------------------------------"
    msg += "\n"
    msg += "The details of the downloaded batch are:\n"
    msg += (
        "    Ex. storage\t\t: {}\n"
    ).format(str(backend_object.get_id()))
    msg += (
        "    Batch id\t\t: {}\n"
    ).format(str(get_req.migration.pk))
    msg += (
        "    Workspace\t\t: {}\n"
    ).format(get_req.migration.workspace)
    msg += (
        "    Label\t\t\t: {}\n"
    ).format(get_req.migration.label)
    msg += (
        "    Date\t\t\t: {}\n"
    ).format(get_req.migration.registered_date.isoformat()[0:16].replace("T"," "))
    msg += (
        "    Stage\t\t\t: {}\n"
    ).format(Migration.STAGE_LIST[get_req.migration.stage])
    # we should have at least one file in the filelist here
    msg += (
        "    Filelist\t: {}\n"
    ).format(get_req.migration.formatted_filelist()[0] + "...")
    msg += (
        "    External batch id\t\t: {}\n"
    ).format(get_req.migration.external_id)
    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def monitor_put(completed_PUTs, backend_object):
    """Monitor the PUTs and MIGRATES and transition from PUTTING to
    VERIFY_PENDING (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__shade=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # check whether it's in the completed_PUTs
        if pr.migration.external_id in completed_PUTs:
            # if it is then migrate to VERIFY_PENDING
            pr.migration.stage = Migration.VERIFY_PENDING
            pr.migration.save()
            logging.info(
                "Transition: batch ID: {} PUTTING->VERIFY_PENDING"
            ).format(pr.migration.external_id))
            # reset the last_archive - needed for verify_get
            pr.last_archive = 0
            pr.save()


def monitor_get(completed_GETs, backend_object):
    """Monitor the GETs and transition from GETTING to ON_DISK (or FAILED)"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(stage=MigrationRequest.GETTING)
        & Q(migration__storage__storage=storage_id=storage_id)
    )

    for gr in get_reqs:
        if gr.migration.external_id in completed_GETs:
            # There may be multiple completed_GETs with external_id as Migrations
            # can be downloaded by multiple MigrationRequests
            # The only way to check is to make sure all the files in the
            # original migration are present in the target_dir
            gr.stage = MigrationRequest.ON_DISK
            send_get_notification_email(gr, backend_object)
            # reset the last archive counter
            gr.last_archive = 0
            gr.save()
            logging.info(
                "Transition: request ID: {} GETTING->ON_DISK"
            ).format(gr.pk))


def monitor_verify(completed_GETs, backend_object):
    """Monitor the VERIFYs and transition from VERIFY_GETTING to VERIFYING"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    verify_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration_stage=Migration.VERIFY_GETTING)
        & Q(migration_storage_storage=storage_id)
    )

    for vr in verify_reqs:
        # This is fine (in contrast) to above monitor_get as
        # 1. There is only one GET for each external_id in the VERIFY stage
        # 2. GETs (for none-VERIFY stage, i.e. to actaully download the data)
        # cannot be issued until the Migration.status is ON_STORAGE
        if vr.migration.external_id in completed_GETs:
            vr.migration.stage = Migration.VERIFYING
            vr.migration.save()
            logging.info(
                "Transition: batch ID: {} VERIFY_GETTING->VERIFYING"
            ).format(vr.migration.external_id))
            # reset the last archive counter
            vr.last_archive = 0
            vr.save()


def run():
    setup_logging(__name__)
    # monitor the backends for completed GETs and PUTs (to et)
    # have to monitor each backend
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        completed_PUTs, completed_GETs = backend_object.monitor()
        # monitor the puts and the gets
        monitor_put(completed_PUTs, backend_object)
        monitor_get(completed_GETs, backend_object)
        monitor_verify(completed_GETs, backend_object)
