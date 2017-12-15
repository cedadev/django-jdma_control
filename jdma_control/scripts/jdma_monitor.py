"""Functions to monitor the files in a request to (PUT) / from (GET) external storage,
   using the current backend monitor function Backend.Monitor.
   A notification email will be sent on GET / PUT completion.

   Running this will change the state of the migrations:
     PUTTING->VERIFY_PENDING
     VERIFYING->ON_STORAGE
     GETTING->ON_DISK
"""

import logging
import subprocess
from datetime import datetime
import calendar

from django.core.mail import send_mail

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.scripts.jdma_lock import setup_logging
from jdma_control.scripts.jdma_verify import get_permissions_string

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_control.backends

def send_get_notification_email(get_req):
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
    subject = "[JDMA] - Notification of batch download from " + settings.JDMA_BACKEND_OBJECT.get_name()
    date = datetime.utcnow()
    date_string = "% 2i %s %d %02d:%02d" % (date.day, calendar.month_abbr[date.month], date.year, date.hour, date.minute)

    msg = "GET request has succesfully completed downloading from " + settings.JDMA_BACKEND_OBJECT.get_name() + "\n"
    msg+= "    Request id\t\t: " + str(get_req.pk)+"\n"
    msg+= "    Stage\t\t\t: " + MigrationRequest.REQ_STAGE_LIST[get_req.stage]+"\n"
    msg+= "    Date\t\t\t: " + get_req.date.isoformat()[0:16].replace("T"," ")+"\n"
    msg+= "    Target path\t\t: " + get_req.target_path+"\n"
    msg+= "\n"
    msg+= "------------------------------------------------"
    msg+= "\n"
    msg+= "The details of the downloaded batch are:\n"

    msg+= "    Batch id\t\t: " + str(get_req.migration.pk)+"\n"
    msg+= "    Workspace\t\t: " + get_req.migration.workspace+"\n"
    msg+= "    Label\t\t\t: " + get_req.migration.label+"\n"
    msg+= "    Date\t\t\t: " + get_req.migration.registered_date.isoformat()[0:16].replace("T"," ")+"\n"
    msg+= "    Permission\t\t: " + Migration.PERMISSION_LIST[get_req.migration.permission]+"\n"
    msg+= "    Original path\t: " + get_req.migration.original_path+"\n"
    msg+= "    Unix uid\t\t: " + get_req.migration.unix_user_id+"\n"
    msg+= "    Unix gid\t\t: " + get_req.migration.unix_group_id+"\n"
    msg+= "    Unix filep\t\t: " + get_permissions_string(get_req.migration.unix_permission)+"\n"
    msg+= "    External id\t\t: " + str(get_req.migration.external_id) + "\n"

    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def monitor_put(completed_PUTs):
    """Monitor the PUTs and transition from PUTTING to VERIFY_PENDING (or FAILED)"""
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for pr in put_reqs:
        if pr.migration.stage == Migration.PUTTING:
            # check whether it's in the completed_PUTs
            if pr.migration.external_id in completed_PUTs:
                # if it is then migrate to VERIFY_PENDING
                pr.migration.stage = Migration.VERIFY_PENDING
                pr.migration.save()
                logging.info("Transition: batch ID: {} PUTTING->VERIFY_PENDING".format(pr.migration.external_id))


def monitor_get(completed_GETs):
    """Monitor the GETs and transition from GETTING to ON_DISK (or FAILED)"""
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    # get the ldap servers
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

    for gr in get_reqs:
        if gr.stage == MigrationRequest.GETTING:
            if gr.migration.external_id in completed_GETs:
                # There may be multiple completed_GETs with external_id as Migrations
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
                        continue
                    # use just the first returned result
                    q = query[0]
                    # # check that the keys exist in q
                    if not ("uidNumber" in q):
                        logging.error("uidNumber not in returned LDAP query for user id {}".format(gr.migration.unix_user_id))
                        continue
                    else:
                        uidNumber = q["uidNumber"][0]

                    # query for the group
                    query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn=gr.migration.unix_group_id)
                    # check for a valid return
                    if len(query) == 0:
                        logging.error("Unix group id: {} not found from LDAP in monitor_get".format(gr.migration.unix_group_id))
                    # use just the first returned result
                    q = query[0]
                    # check that the keys exist in q
                    if not ("gidNumber" in q):
                        logging.error("gidNumber not in returned LDAP query for group id {}".format(gr.migration.unix_group_id))
                        continue
                    else:
                        gidNumber = q["gidNumber"][0]

                    # change the directory owner / group
                    subprocess.call(["/usr/bin/sudo", "/bin/chown", "-R", str(uidNumber)+":"+str(gidNumber), gr.target_path])

                    # change the permissions back to the original
                    subprocess.call(["/usr/bin/sudo", "/bin/chmod", "-R", oct(gr.migration.unix_permission)[2:], gr.target_path])

                gr.stage = MigrationRequest.ON_DISK
                send_get_notification_email(gr)

                gr.save()
                logging.info("Transition: request ID: {} GETTING->ON_DISK".format(gr.pk))


def monitor_verify(completed_GETs):
    """Monitor the VERIFYs and transition from VERIFY_GETTING to VERIFYING"""
    verify_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT)
    for vr in verify_reqs:
        if vr.migration.stage == Migration.VERIFY_GETTING:
            # This is fine (in contrast) to above monitor_get as
            # 1. There is only one GET for each external_id in the VERIFY stage
            # 2. GETs (for none-VERIFY stage, i.e. to actaully download the data)
            # cannot be issued until the Migration.status is ON_STORAGE
            if vr.migration.external_id in completed_GETs:
                vr.migration.stage = Migration.VERIFYING
                vr.migration.save()
                logging.info("Transition: batch ID: {} VERIFY_GETTING->VERIFYING".format(vr.migration.external_id))


def run():
    setup_logging(__name__)
    # monitor the backend for completed GETs and PUTs (to et)
    completed_PUTs, completed_GETs = settings.JDMA_BACKEND_OBJECT.monitor()
    # monitor the puts and the gets
    monitor_put(completed_PUTs)
    monitor_get(completed_GETs)
    monitor_verify(completed_GETs)
