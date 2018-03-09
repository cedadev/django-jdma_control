"""Functions to verify files that have been migrated to external storage.
   These files have been put on external storage and then (temporarily) pulled back to
   disk before being verified by calculating the SHA256 digest and comparing it
   to the digest that was calculated (in jdma_transfer) before it was uploaded
   to external storage.
   Running this will change the state of the migrations:
     VERIFYING->ON_STORAGE
"""

import os
import sys
import logging

from django.core.mail import send_mail
from django.db.models import Q

from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging, calculate_digest
from jdma_control.scripts.jdma_transfer import mark_migration_failed
import jdma_control.backends

def get_permissions_string(p):
    # this is unix permissions
    is_dir = 'd'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = oct(p)[-3:]
    return is_dir + ''.join(dic.get(x,x) for x in perm)


def send_put_notification_email(put_req, backend_object):
    """Send an email to the user to notify them that their batch upload has
    been completed.
    """
    user = put_req.user

    if not user.notify:
        return

    # to address is notify_on_first
    toaddrs = [user.email]
    # from address is just a dummy address
    fromaddr = "support@ceda.ac.uk"

    # subject
    subject = (
        "[JDMA] - Notification of batch upload to external storage {}"
    ).format(backend_object.get_name())

    msg = (
        "PUT request has succesfully completed uploading to external storage: "
        "{}\n"
    ).format(backend_object.get_name())
    msg += (
        "    Request id\t\t: {}\n"
    ).format(put_req.pk)
    msg += (
        "    Ex. storage\t\t: {}\n"
    ).format(str(backend_object.get_id()))
    msg += (
        "    Batch id\t\t: {}\n"
    ).format(str(put_req.migration.pk))
    msg += (
        "    Workspace\t\t: {}\n"
    ).format(put_req.migration.workspace)
    msg += (
        "    Label\t\t\t: {}\n"
    ).format(put_req.migration.label)
    msg += (
        "    Date\t\t\t: {}\n"
    ).format(put_req.migration.registered_date.isoformat()[0:16].replace("T"," "))
    msg += (
        "    Stage\t\t\t: {}\n"
    ).format(Migration.STAGE_LIST[put_req.migration.stage])
    # we should have at least one file in the filelist here
    msg += (
        "    Filelist\t: {}\n"
    ).format(put_req.migration.formatted_filelist()[0] + "...")
    msg += (
        "    External batch id\t\t: {}\n"
    ).format(put_req.migration.external_id)

    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def verify_files(backend_object):
    """Verify the files that have been uploaded to external storage and then
    downloaded back to a temporary directory."""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # these are part of a PUT request - get the list of PUT request
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # Check whether the request is in the verifying stage
        if pr.migration.stage == Migration.VERIFYING:
            # get the batch id
            external_id = pr.migration.external_id
            # get the temporary directory
            verify_dir = os.path.join(
                backend_object.VERIFY_DIR,
                "verify_{}".format(pr.migration.external_id)
            )
            # loop over the MigrationArchives that belong to this Migration
            archive_set = pr.migration.migrationarchive_set.order_by('pk')
            # first check that each file exists in the temporary directory
            for archive in archive_set:
                # filename is concatenation of verify_dir and the the original
                # file path
                verify_file_path = os.path.join(
                    verify_dir,
                    archive.get_id()
                ) + ".tar"
                # check the file exists - if it doesn't then set the stage to
                # FAILED and write that the file couldn't be found in the
                # failure_reason
                if not os.path.exists(verify_file_path):
                    failure_reason = (
                        "VERIFY: archive {} could not be found."
                    ).format(verify_file_path)
                    mark_migration_failed(pr, failure_reason)
                    sys.exit(0)
                else:
                    # check that the digest matches
                    new_digest = calculate_digest(verify_file_path)
                    # check that the digests match
                    if new_digest != archive.digest:
                        failure_reason = (
                            "VERIFY: archive {} has a different digest."
                        ).format(verify_file_path)
                        mark_migration_failed(pr, failure_reason)
                        sys.exit(0)
            # if we reach this part without exiting then the batch has verified
            # successfully and we can transition to ON_STORAGE, ready for the
            # tidy up process
            # Transition Migration
            pr.migration.stage = Migration.ON_STORAGE
            send_put_notification_email(pr, backend_object)
            pr.migration.save()
            # Transition MigrationRequest
            pr.stage = MigrationRequest.ON_STORAGE
            pr.save()
            logging.info((
                "Transition: batch ID: {} VERIFYING->ON_STORAGE"
            ).format(pr.migration.external_id))

def run():
    setup_logging(__name__)
    # loop over all backends - should we parallelise these?
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        verify_files(backend_object)
