"""Functions to tidy up after the JDMA has migrated data to external storage.
   It will do the following:
   1.  Delete the verification directory and all its contents
   2.  Delete the file list in /jdma_file_lists
   3.  Delete the digest in /jdma_file_lists
   4.  Delete the original directory and all its contents (!)
   5.  Remove the migration request (but leave the migration)
   Running this will not change the state of any of the migrations.
"""

import os
import logging
import shutil
import subprocess

from django.db.models import Q
from django.core.mail import send_mail

from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging

import jdma_control.backends

def send_put_notification_email(backend_object, put_req):
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


def send_get_notification_email(get_req, backend_object):
    """Send an email to the user to notify them that their batch upload has been
     completed
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


def remove_archive_files(backend_object, pr):
    """Remove the temporary tar_files that were created when uploading to the
    external storage"""
    # get the directory that the temporary files are in
    batch_id = pr.migration.external_id
    # get the staging directory
    archive_dir = os.path.join(
        backend_object.ARCHIVE_STAGING_DIR,
        batch_id
    )
    # remove the directory
    if os.path.isdir(archive_dir):
        shutil.rmtree(archive_dir)
        logging.info("TIDY: deleting archive directory " + archive_dir)
    else:
        logging.error("TIDY: cannot find archive directory " + archive_dir)


def remove_verification_files(backend_object, pr):
    """Remove those temporary files that have been created in the verification step"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # get the directory that the temporary files are in
    batch_id = pr.migration.external_id
    # get the temporary directory
    verify_dir = os.path.join(
        backend_object.VERIFY_DIR, "batch{}".format(batch_id)
    )
    # remove the directory
    if os.path.isdir(verify_dir):
        shutil.rmtree(verify_dir)
        logging.info("TIDY: deleting verify directory " + verify_dir)
    else:
        logging.error("TIDY: cannot find verify directory " + verify_dir)


def remove_original_files(backend_object, pr):
    """Remove the original files.
    This will occur if request_type == MIGRATE
    This is the whole point of the migration!"""
    # loop over the files in the filelist
    for fd in pr.migration.filelist:
        # check whether it's a directory: walk if it is
        if os.path.isdir(fd):
            # delete the whole directory!
            try:
                shutil.rmtree(fd)
                logging.info((
                    "TIDY: deleting directory {}"
                ).format(fd))
            except Exception as e:
                logging.info((
                    "TIDY: could not delete directory {} : {}"
                ).format(fd, str(e)))
        else:
            try:
                os.unlink(fd)
                logging.info((
                    "TIDY: deleting file {}"
                ).format(fd))
            except Exception as e:
                logging.info((
                    "TIDY: could not delete file {} : {}"
                ).format(fd, str(e)))


def unlock_original_files(backend_object, pr):
    """Restore the uid:gid and permissions on the original files.
    This will occur if request_type == PUT"""
    # loop over the MigrationArchives that belong to this Migration
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    # use last_archive to enable restart of unlock
    st_arch = pr.last_archive
    n_arch = archive_set.count()
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # get the migrationfiles from the archive
        files_set = archive.migrationfile_set.order_by('pk')
        for f in files_set:
            # change the owner of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chown", "-R",
                 "{}:{}".format(f.unix_user_id, f.unix_group_id),
                 f.path])
            # change the permissions of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chmod", "-R",
                 "{}".format(f.unix_permission),
                 f.path]
            )
        pr.last_archive += 1
        pr.save()


def remove_put_files(backend_object, pr):
    """Run the file removals in order"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    try:
        remove_archive_files(backend_object, pr)
        remove_verification_files(backend_object, pr)
        # only remove the original files for a MIGRATE
        if pr.request_type == MigrationRequest.MIGRATE:
            remove_original_files(backend_object, pr)
        else:
            unlock_original_files(backend_object, pr)
        # set to completed and last archive to 0
        # pr will be deleted next time jdma_tidy is invoked
        #pr.stage = MigrationRequest.PUT_COMPLETED
        pr.last_archive = 0
        pr.save()

    except Exception as e:
        raise Exception(e)


def remove_put_requests(backend_object):
    """Remove the put requests that are ON_STORAGE"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_COMPLETED)
    )
    for pr in put_reqs:
        logging.info("TIDY: deleting PUT request {}".format(pr.pk))
        pr.delete()


def remove_get_requests(backend_object):
    """Remove the get requests that are ON_DISK"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend.
    # This involves resolving two foreign keys
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.GET_COMPLETED)
    )
    for gr in get_reqs:
        # only do it if the files are on external storage
        logging.info("TIDY: deleting GET request {}".format(gr.pk))
        gr.delete()


def update_storage_quota(backend, pr):
    """Update the storage quota for a completed PUT request for this backend.
    """
    # update the storage quota for the user by adding up all the archives and
    # subtracting it from the quota
    archive_sum = 0
    archives = pr.migration.migrationarchive_set.all()
    for arch in archives:
        archive_sum += arch.size
    quota = pr.migration.storage
    quota.quota_used += archive_sum
    quota.save()


def PUT_completed(backend_object):
    """Do the clean up tasks for a completed PUT or MIGRATE request"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # these occur during a PUT or MIGRATE request
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_TIDY))
    for pr in put_reqs:
        try:
            # send a notification email that the puts have completed
            send_put_notification_email(backend_object, pr)
            # update the amount of quota the migration has used
            update_storage_quota(backend_object, pr)
            # remove the temporary staged archive files
            remove_archive_files(backend_object, pr)
            # remove the verification files
            remove_verification_files(backend_object, pr)
            # only remove the original files for a MIGRATE
            if pr.request_type == MigrationRequest.MIGRATE:
                remove_original_files(backend_object, pr)
            else:
            # otherwise unlock them (restore uids, gids and permissions)
                unlock_original_files(backend_object, pr)
            # set to completed and last archive to 0
            # pr will be deleted next time jdma_tidy is invoked
            pr.stage = MigrationRequest.PUT_COMPLETED
            pr.migration.stage = Migration.ON_STORAGE
            pr.last_archive = 0
            pr.migration.save()
            pr.save()
        except Exception as e:
            logging.error("TIDY: error in PUT_completed {}".format(str(e)))
            raise Exception(e)


def run():
    setup_logging(__name__)
    # these are individual loops to aid debugging and so we can turn them
    # on / off if we wish
    # loop over all backends
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        # run in this order so that MigrationRequests are not deleted immediately
        remove_put_requests(backend_object)
        remove_get_requests(backend_object)

        PUT_completed(backend_object)
