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

def get_batch_info_for_email(backend_object, migration):
    msg = ""
    msg += "The details of the downloaded batch are:\n"
    msg += (
        "    Ex. storage\t\t: {}\n"
    ).format(str(backend_object.get_id()))
    msg += (
        "    Batch id\t\t: {}\n"
    ).format(str(migration.pk))
    msg += (
        "    Workspace\t\t: {}\n"
    ).format(migration.workspace)
    msg += (
        "    Label\t\t\t: {}\n"
    ).format(migration.label)
    msg += (
        "    Date\t\t\t: {}\n"
    ).format(migration.registered_date.isoformat()[0:16].replace("T"," "))
    msg += (
        "    Stage\t\t\t: {}\n"
    ).format(Migration.STAGE_LIST[migration.stage])
    # we should have at least one file in the filelist here
    msg += (
        "    Filelist\t: {}\n"
    ).format(migration.formatted_filelist()[0] + "...")
    msg += (
        "    External batch id\t\t: {}\n"
    ).format(migration.external_id)
    return msg


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

    msg += get_batch_info_for_email(backend_object, put_req.migration)

    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def send_get_notification_email(backend_object, get_req):
    """Send an email to the user to notify them that their batch upload has been
     completed
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

    msg += get_batch_info_for_email(backend_object, get_req.migration)

    send_mail(subject, msg, fromaddr, toaddrs, fail_silently=False)


def send_delete_notification_email(backend_object, del_req):
    """Send an email to the user to notify them that their batch has been
    succssfully deleted.
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
        "[JDMA] - Notification of deletion from external storage {}"
    ).format(backend_object.get_name())

    msg = (
        "DELETE request has succesfully completed deleting from external storage: "
        "{}\n"
    ).format(backend_object.get_name())

    msg += (
        "    Request id\t\t: {}\n"
    ).format(del_req.pk)

    msg += get_batch_info_for_email(backend_object, del_req.migration)

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
        backend_object.VERIFY_DIR, "verify_{}".format(batch_id)
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
    common_path = pr.migration.common_path
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # get the migrationfiles from the archive
        files_set = archive.migrationfile_set.order_by('pk')
        for f in files_set:
            # get the full path
            path = os.path.join(pr.migration.common_path, f.path)
            # skip the file if it doesn't exist but log
            if not (os.path.exists(path) or os.path.isdir(path)):
                logging.error((
                        "Unlock files: path does not exist {} "
                    ).format(path)
                )
                continue
            # change the owner of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chown", "-R",
                 "{}:{}".format(f.unix_user_id, f.unix_group_id),
                 path])
            # change the permissions of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chmod", "-R",
                 "{}".format(f.unix_permission),
                 path]
            )
        pr.last_archive += 1
        pr.save()
    # unlock the common path
    if common_path != None:
        if not (os.path.exists(common_path) or os.path.isdir(common_path)):
            logging.error((
                    "Unlock files: path does not exist {} "
                ).format(common_path)
            )
        else:
            # change the owner of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chown", "-R",
                 "{}:{}".format(
                    pr.migration.common_path_user_id,
                    pr.migration.common_path_group_id
                 ),
                 common_path])
            # change the permissions of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chmod", "-R",
                 "{}".format(pr.migration.common_path_permission),
                 common_path]
            )


def remove_put_request(pr):
    """Remove a put request that is PUT_COMPLETED and the associated
    migration is ON_STORAGE"""
    logging.info("TIDY: deleting PUT request {}".format(pr.pk))
    pr.delete()


def remove_get_request(gr):
    """Remove the get requests that are GET_COMPLETED"""
    logging.info("TIDY: deleting GET request {}".format(gr.pk))
    gr.delete()


def remove_delete_request(dr):
    """Remove the delete requests that are DELETE_COMPLETED"""
    logging.info("TIDY: deleting DELETE request {}".format(dr.pk))
    dr.delete()

def update_storage_quota(backend, migration, update="add"):
    """Update the storage quota for a completed PUT request for this backend.
    """
    # update the storage quota for the user by adding up all the archives and
    # subtracting it from the quota
    archive_sum = 0
    archives = migration.migrationarchive_set.all()
    for arch in archives:
        archive_sum += arch.size
    quota = migration.storage
    # add or delete?
    if update == "add":
        quota.quota_used += archive_sum
    elif update == "delete":
        quota.quota_used -= archive_sum
    quota.save()


def PUT_tidy(backend_object):
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
            # check locked
            if pr.locked:
                continue
            pr.lock()
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
            # unlock
            pr.locked = False
            pr.save()
        except Exception as e:
            logging.error("TIDY: error in PUT_tidy {}".format(str(e)))


def GET_tidy(backend_object):
    """Do the clean up tasks for a completed GET request"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # these occur during a PUT or MIGRATE request
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.GET_TIDY)
        )
    for gr in get_reqs:
        try:
            if gr.locked:
                continue
            gr.lock()
            # remove the temporary archive files (tarfiles)
            remove_archive_files(backend_object, gr)
            # update the request to GET_COMPLETED
            gr.stage = MigrationRequest.GET_COMPLETED
            gr.last_archive = 0
            gr.locked = False
            gr.save()
        except Exception as e:
            logging.error("TIDY: error in GET_tidy {}".format(str(e)))


def DELETE_tidy(backend_object):
    """Do the tasks to tidy up a DELETE request:
    Delete the temporary archive files if the associated migration stage
     > PUT_PACKING
    Delete the temporary verify files if the associated migration stage
     > VERIFY_PENDING
    Restore permissions on original files if request type == PUT and the stage
     < PUT_TIDY (if after PUT_TIDY the permissions will have been restored for
     a PUT request, or deleted for a MIGRATE request)
    """
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # these occur during a PUT or MIGRATE request
    del_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.DELETE_TIDY)
        )
    for dr in del_reqs:
        try:
            if dr.locked:
                continue
            dr.lock()
            # get the associated PUT or MIGRATE requests - there should only be
            # zero or one
            put_reqs = MigrationRequest.objects.filter(
                (Q(request_type=MigrationRequest.PUT)
                | Q(request_type=MigrationRequest.MIGRATE))
                & Q(migration=dr.migration)
                & Q(migration__storage__storage=storage_id)
            )
            # loop over these requests and act on the stage of that request
            for pr in put_reqs:
                # switch on the stage of the associate request
                if pr.stage > MigrationRequest.PUT_PACKING:
                    # remove the temporary staged archive files
                    remove_archive_files(backend_object, pr)
                if pr.stage > MigrationRequest.VERIFY_PENDING:
                    # remove the verification files
                    remove_verification_files(backend_object, pr)
                if pr.stage < MigrationRequest.PUT_TIDY:
                    unlock_original_files(backend_object, pr)

            # get the associate GET requests - there could be many or none
            get_reqs = MigrationRequest.objects.filter(
                Q(request_type=MigrationRequest.GET)
                & Q(migration=dr.migration)
                & Q(migration__storage__storage=storage_id)
            )
            # loop over these requests and find those that stage > GET_PENDING
            for gr in get_reqs:
                if gr.stage > MigrationRequest.GET_PENDING:
                    # remove the temporary staged archive files
                    remove_archive_files(backend_object, gr)

            # update the request to DELETE_COMPLETED
            dr.stage = MigrationRequest.DELETE_COMPLETED
            dr.last_archive = 0
            dr.locked = False
            dr.save()
        except Exception as e:
            logging.error("TIDY: error in GET_tidy {}".format(str(e)))


def PUT_completed(backend_object):
    """Do the tasks for a completed PUT request:
        send a notification email
        update the quota
        delete the request
    """
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_COMPLETED)
    )
    for pr in put_reqs:
        try:
            # check locked
            if pr.locked:
                continue
            pr.lock()
            # send a notification email that the puts have completed
            send_put_notification_email(backend_object, pr)
            # update the amount of quota the migration has used
            update_storage_quota(backend_object, pr.migration, update="add")
            # unlock
            pr.unlock()
            # delete the request
            remove_put_request(pr)
        except Exception as e:
            logging.error("TIDY: error in PUT_completed {}".format(str(e)))


def GET_completed(backend_object):
    """Do the tasks for a completed GET request:
       send a notification email
       delete the request
    """
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.GET_COMPLETED)
    )
    for gr in get_reqs:
        try:
            if gr.locked:
                continue
            # lock
            gr.lock()
            # send a notification email that the gets have completed
            send_get_notification_email(backend_object, gr)
            # unlock
            gr.unlock()
            remove_get_request(gr)
        except Exception as e:
            logging.error("TIDY: error in GET_completed {}".format(str(e)))


def DELETE_completed(backend_object):
    """Do the tasks for a completed DELETE request:
       send a notification email
       delete the request
       delete the migration
       delete any associated requests (PUT, MIGRATE or GETs)
    """
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # these occur during a PUT or MIGRATE request
    del_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.DELETE_COMPLETED)
        )
    for dr in del_reqs:
        try:
            if dr.locked:
                continue
            dr.lock()
            # get the associated PUT or MIGRATE requests - there should only be
            # one
            other_reqs = MigrationRequest.objects.filter(
                (Q(request_type=MigrationRequest.PUT)
                | Q(request_type=MigrationRequest.MIGRATE)
                | Q(request_type=MigrationRequest.GET))
                & Q(migration=dr.migration)
                & Q(migration__storage__storage=storage_id)
            )
            for otr in other_reqs:
                logging.info((
                    "TIDY: deleting request {} associated with DELETE request {}."
                ).format(otr.pk, dr.pk))
                otr.delete()
            # update the quota
            update_storage_quota(backend_object, dr.migration, update="delete")
            # delete the migration
            logging.info((
                "TIDY: deleting migration {} associated with DELETE request {}."
            ).format(dr.migration.pk, dr.pk))
            dr.migration.delete()
            # delete the delete request
            logging.info((
                "TIDY: deleting DELETE request {}."
            ).format(dr.pk))
            dr.delete()
            # we are done!
        except Exception as e:
            logging.error("TIDY: error in DELETE_completed {}".format(str(e)))


def process(backend):
    backend_object = backend()
    # run in this order so that MigrationRequests are not deleted immediately
    # which aids debugging!
    PUT_completed(backend_object)
    GET_completed(backend_object)
    DELETE_completed(backend_object)

    PUT_tidy(backend_object)
    GET_tidy(backend_object)
    DELETE_tidy(backend_object)


def run(*args):
    # setup the logging
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
