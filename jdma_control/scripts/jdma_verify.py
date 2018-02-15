"""Functions to verify files that have been migrated to external storage.
   These files have been put on external storage and then (temporarily) pulled back to
   disk before being verified by calculating the SHA256 digest and comparing it
   to the digest that was calculated (in jdma_transfer) before it was uploaded
   to external storage.
   Running this will change the state of the migrations:
     VERIFYING->ON_STORAGE
"""

import os
import subprocess
import logging
import hashlib
from datetime import datetime
import calendar

from django.core.mail import send_mail

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, MigrationFile
from jdma_control.scripts.jdma_lock import setup_logging
import jdma_control.backends

def get_permissions_string(p):
    # this is unix permissions
    is_dir = 'd'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = oct(p)[-3:]
    return is_dir + ''.join(dic.get(x,x) for x in perm)


def send_put_notification_email(put_req, backend_object):
    """Send an email to the user to notify them that their batch upload has been completed
     var jdma_control.models.User user: user to send notification email to
    """
    user = put_req.user

    if not user.notify:
        return

    # to address is notify_on_first
    toaddrs = [user.email]
    # from address is just a dummy address
    fromaddr = "support@ceda.ac.uk"

    # subject
    subject = "[JDMA] - Notification of batch upload to external storage " + backend_object.get_name()
    date = datetime.utcnow()
    date_string = "% 2i %s %d %02d:%02d" % (date.day, calendar.month_abbr[date.month], date.year, date.hour, date.minute)

    msg = "PUT request has succesfully completed uploading to external storage: " + backend_object.get_name() + "\n"
    msg+= "    Request id\t\t: " + str(put_req.pk)+"\n"
    msg+= "    Ex. storage\t\t: " + str(backend_object.get_id())+"\n"
    msg+= "    Batch id\t\t: " + str(put_req.migration.pk)+"\n"
    msg+= "    Workspace\t\t: " + put_req.migration.workspace+"\n"
    msg+= "    Label\t\t\t: " + put_req.migration.label+"\n"
    msg+= "    Date\t\t\t: " + put_req.migration.registered_date.isoformat()[0:16].replace("T"," ")+"\n"
    msg+= "    Stage\t\t\t: " + Migration.STAGE_LIST[put_req.migration.stage]+"\n"
    msg+= "    Original path\t: " + put_req.migration.original_path+"\n"
    msg+= "    Unix uid\t\t: " + put_req.migration.unix_user_id+"\n"
    msg+= "    Unix gid\t\t: " + put_req.migration.unix_group_id+"\n"
    msg+= "    Unix filep\t\t: " + get_permissions_string(put_req.migration.unix_permission)+"\n"
    msg+= "    External batch id\t\t: " + str(put_req.migration.external_id) + "\n"

    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def calculate_digest(filename):
    # Calculate the hex digest of the file, using a buffer
    BUFFER_SIZE = 256 * 1024 # (256KB) - adjust this

    # create a sha256 object
    sha256 = hashlib.sha256()

    # read through the file
    with open(filename, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)
            if not data: # EOF
                break
            sha256.update(data)
    return "SHA256: {0}".format(sha256.hexdigest())


def verify_files(backend_object):
    """Verify the files that have been uploaded to external storage and then downloaded
    back to a temporary directory."""
    # these are part of a PUT request
    put_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.PUT,
                                               storage=backend_object.get_id())
    for pr in put_reqs:
        # Check whether the request is in the verifying stage
        if pr.migration.stage == Migration.VERIFYING:
            # get the batch id
            external_id = pr.migration.external_id
            # get the temporary directory
            verify_dir = os.path.join(backend_object.VERIFY_DIR, "batch{}".format(external_id))
            # loop over the MigrationFiles that belong to this Migration
            migration_files = MigrationFile.objects.filter(migration=pr.migration)
            # first check that each file exists in the temporary directory
            for mf in migration_files:
                # filename is concatenation of verify_dir and the the original file path
                verify_file_path = verify_dir + mf.path     # note cannot use join here as paths may be defined from /
                # check the file exists - if it doesn't then set the stage to FAILED
                # and write that the file couldn't be found in the failure_reason
                if not os.path.exists(verify_file_path):
                    pr.migration.stage = Migration.FAILED
                    pr.migration.failure_reason = "VERIFY: file " + verify_file_path + " could not be found."
                    logging.error("VERIFY: " + pr.migration.failure_reason)
                    pr.migration.save()
                    sys.exit(0)
                else:
                    # check that the digest matches
                    new_digest = calculate_digest(verify_file_path)[len(" SHA256:"):]
                    # check that the digests match
                    if new_digest != mf.digest:
                        # if not then indicate via the failure_reason
                        pr.migration.stage = Migration.FAILED
                        pr.migration.failure_reason = "VERIFY: file " + verify_file_path + " has a different digest."
                        logging.error("VERIFY: " + pr.migration.failure_reason)
                        pr.migration.save()
                        sys.exit(0)
            # if we reach this part without exiting then the batch has verified successfully and we
            # can transition to ON_STORAGE, ready for the tidy up process
            pr.migration.stage = Migration.ON_STORAGE
            send_put_notification_email(pr)
            pr.migration.save()
            logging.info("Transition: batch ID: {} VERIFYING->ON_STORAGE".format(pr.migration.external_id))

def run():
    setup_logging(__name__)
    for backend in jdma_control.backends.get_backends():
        verify_files(backend)
