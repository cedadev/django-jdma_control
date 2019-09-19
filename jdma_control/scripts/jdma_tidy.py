"""Functions to tidy up after the JDMA has migrated data to external storage.
   A notification email will be sent on GET / PUT completion.
"""

import os
import logging
import shutil
import subprocess
import signal,sys
from time import sleep
import datetime
from multiprocessing import Process

from django.db.models import Q
from django.core.mail import send_mail

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.common import get_verify_dir, get_staging_dir, get_download_dir
from jdma_control.scripts.common import split_args

import jdma_control.backends
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level

def get_batch_info_for_email(backend_object, migration):
    msg = ""
    msg += "The details of the downloaded batch are:\n"
    msg += (
        "    Ex. storage\t\t\t: {}\n"
    ).format(str(backend_object.get_id()))
    msg += (
        "    Batch id\t\t\t: {}\n"
    ).format(str(migration.pk))
    msg += (
        "    Workspace\t\t\t: {}\n"
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
        "    Request id\t\t\t: {}\n"
    ).format(put_req.pk)

    msg += "\n"
    msg += "------------------------------------------------"
    msg += "\n"

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
        "    Request id\t\t\t: {}\n"
    ).format(str(get_req.pk))
    msg += (
        "    Stage\t\t\t: {}\n"
    ).format(MigrationRequest.REQ_STAGE_LIST[get_req.stage])
    msg += (
        "    Date\t\t\t: {}\n"
    ).format(get_req.date.isoformat()[0:16].replace("T"," "))
    msg += (
        "    Target path\t\t\t: {}\n"
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
    user = del_req.user

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
    # get the untarring directory
    if ((pr.request_type == MigrationRequest.PUT) |
        (pr.request_type == MigrationRequest.MIGRATE)):
        archive_dir = get_staging_dir(backend_object, pr)
    else:
        archive_dir = get_download_dir(backend_object, pr)
    # remove the directory
    if os.path.isdir(archive_dir):
        shutil.rmtree(archive_dir)
        logging.info("Deleting archive directory " + archive_dir)
    else:
        logging.error("Cannot find archive directory " + archive_dir)


def remove_verification_files(backend_object, pr):
    """Remove those temporary files that have been created in the verification
    step"""
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # get the directory that the temporary files are in
    batch_id = pr.migration.external_id
    # get the temporary directory
    verify_dir = get_verify_dir(backend_object, pr)
    # remove the directory
    if os.path.isdir(verify_dir):
        shutil.rmtree(verify_dir)
        logging.info("Deleting verify directory " + verify_dir)
    else:
        logging.error("Cannot find verify directory " + verify_dir)


def remove_original_file_list(filelist):
    """Remove files given in a file list"""
    for fd in filelist:
        # check whether it's a directory: walk if it is
        if os.path.isdir(fd):
            # delete the whole directory!
            try:
                shutil.rmtree(fd)
                logging.info((
                    "Deleting directory {}"
                ).format(fd))
            except Exception as e:
                logging.error((
                    "Could not delete directory {} : {}"
                ).format(fd, str(e)))
        else:
            try:
                os.unlink(fd)
                logging.info((
                    "Deleting file {}"
                ).format(fd))
            except Exception as e:
                logging.error((
                    "Could not delete file {} : {}"
                ).format(fd, str(e)))


def remove_original_files(backend_object, pr, config):
    """Remove the original files.
    This will occur if request_type == MIGRATE
    This is the whole point of the migration!"""
    # loop over the files in the filelist
    filelist = pr.filelist
    if len(filelist) > 0:
        n_threads = config["THREADS"]
        processes = []
        for tn in range(0, n_threads):
            local_filelist = filelist[tn::n_threads]
            p = Process(
                target = remove_original_file_list,
                args = (local_filelist,)
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()


def unlock_file_list(file_info_list):
    """Unlock a list of files.
    file_list is a list of tuples:
        (file_path, unix uid, unix gid, unix permissions)
    """
    for fi in file_info_list:
        # skip the file if it doesn't exist but log
        if not (os.path.exists(fi[0]) or os.path.isdir(fi[0])):
            logging.error((
                    "Unlock files: path does not exist {} "
                ).format(fi[0])
            )
            continue
        # change the owner of the file - doesn't need to be recursive now!
        subprocess.call(
            ["/usr/bin/sudo",
             "/bin/chown",
             "{}:{}".format(fi[1], fi[2]),
             fi[0]])
        # change the permissions of the file - doesn't need to be recursive!
        subprocess.call(
            ["/usr/bin/sudo",
             "/bin/chmod",
             "{}".format(fi[3]),
             fi[0]]
        )

def unlock_original_files(backend_object, pr, config):
    """Restore the uid:gid and permissions on the original files.
    This will occur if request_type == PUT"""
    # loop over the MigrationArchives that belong to this Migration
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    # use last_archive to enable restart of unlock
    st_arch = 0 #pr.last_archive
    n_arch = archive_set.count()
    common_path = pr.migration.common_path
    file_info_list = []
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # get the migrationfiles from the archive
        files_set = archive.migrationfile_set.order_by('pk')
        for f in files_set:
            # get the full path
            path = os.path.join(pr.migration.common_path, f.path)
            # don't do anythin the links - as we didn't change them in the first place
            if not (f.ftype == "LINK" or f.ftype == "LNCM" or f.ftype == "LNAS"):
                # append to the master file list
                file_info_list.append((
                    path,
                    f.unix_user_id,
                    f.unix_group_id,
                    f.unix_permission
                ))

    if len(file_info_list) > 0:
        n_threads = config["THREADS"]
        processes = []
        for tn in range(0, n_threads):
            local_file_info_list = file_info_list[tn::n_threads]
            p = Process(
                target = unlock_file_list,
                args = (local_file_info_list,)
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

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
                 "/bin/chown",
                 "{}:{}".format(
                    pr.migration.common_path_user_id,
                    pr.migration.common_path_group_id
                 ),
                 common_path])
            # change the permissions of the file
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chmod",
                 "{}".format(pr.migration.common_path_permission),
                 common_path]
            )

def update_storage_quota(backend, migration, update="add"):
    """Update the storage quota for a completed PUT request for this backend.
    """
    # update the storage quota for the user by adding up all the archives and
    # subtracting it from the quota
    archive_sum = 0
    archives = migration.migrationarchive_set.all()
    for arch in archives:
        # the size of the tar file will be the same as the individual file
        # as the tar file is not zipped
        archive_sum += arch.size
    quota = migration.storage
    # add or delete?
    if update == "add":
        quota.quota_used += archive_sum
    elif update == "delete":
        quota.quota_used -= archive_sum
    quota.save()


def PUT_tidy(backend_object, config):
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
                remove_original_files(backend_object, pr, config)
            else:
                # otherwise unlock them (restore uids, gids and permissions)
                unlock_original_files(backend_object, pr, config)
            # set to completed and last archive to 0
            # pr will be deleted next time jdma_tidy is invoked
            pr.stage = MigrationRequest.PUT_COMPLETED
            logging.info("Transition: deleting PUT request {}".format(pr.pk))
            pr.migration.stage = Migration.ON_STORAGE
            logging.info((
                "Transition: request ID: {} external ID: {}: PUT_TIDY->PUT_COMPLETED, PUTTING->ON_STORAGE"
            ).format(pr.pk, pr.migration.external_id))
            pr.last_archive = 0
            pr.migration.save()
            # unlock
            pr.unlock()
            # send a notification email that the puts have completed
            send_put_notification_email(backend_object, pr)
            # update the amount of quota the migration has used
            update_storage_quota(backend_object, pr.migration, update="add")

        except Exception as e:
            raise Exception(e)
            logging.error("Error in PUT_tidy {}".format(str(e)))


def GET_tidy(backend_object, config):
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
            logging.info((
                "Transition: request ID: {} external ID: {}: GET_TIDY->GET_COMPLETED"
            ).format(gr.pk, gr.migration.external_id))
            gr.migration.save()
            gr.save()
            gr.unlock()
            # send a notification email that the gets have completed
            send_get_notification_email(backend_object, gr)
        except Exception as e:
            logging.error("GET: error in GET_tidy {}".format(str(e)))


def DELETE_tidy(backend_object, config):
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
                pr.lock()
                # switch on the stage of the associate request
                if (pr.stage > MigrationRequest.PUT_PACKING and
                    pr.stage < MigrationRequest.PUT_COMPLETED):
                    # remove the temporary staged archive files
                    remove_archive_files(backend_object, pr)
                if (pr.stage > MigrationRequest.VERIFY_PENDING and
                    pr.stage < MigrationRequest.PUT_COMPLETED):
                    # remove the verification files
                    remove_verification_files(backend_object, pr)
                if pr.stage < MigrationRequest.PUT_TIDY:
                    unlock_original_files(backend_object, pr, config)
                pr.unlock()

            # get the associate GET requests - there could be many or none
            get_reqs = MigrationRequest.objects.filter(
                Q(request_type=MigrationRequest.GET)
                & Q(migration=dr.migration)
                & Q(migration__storage__storage=storage_id)
            )
            # loop over these requests and find those that stage > GET_PENDING
            for gr in get_reqs:
                if (gr.stage > MigrationRequest.GET_PENDING and
                    gr.stage < MigrationRequest.GET_COMPLETED):
                    pr.lock()
                    # remove the temporary staged archive files
                    remove_archive_files(backend_object, gr)
                    pr.unlock()

            # update the request to DELETE_COMPLETED
            dr.stage = MigrationRequest.DELETE_COMPLETED
            dr.last_archive = 0
            # update the migration stage to DELETED
            dr.migration.stage = Migration.DELETED
            logging.info((
                "Transition: request ID: {} external ID: {}: DELETE_TIDY->DELETE_COMPLETED, DELETING->DELETED"
            ).format(dr.pk, dr.migration.external_id))
            dr.migration.save()
            dr.save()
            # update the quota
            update_storage_quota(backend_object, dr.migration, update="delete")
            send_delete_notification_email(backend_object, gr)
            dr.unlock()
        except Exception as e:
            logging.error("Error in DELETE_tidy {}".format(str(e)))


def PUT_completed(backend_object, config):
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
    now = datetime.datetime.utcnow()
    num_days = datetime.timedelta(days=config["COMPLETED_REQUEST_DAYS"])
    for pr in put_reqs:
        try:
            # check locked
            if pr.locked:
                continue
            # remove the request if the requisite time has elapsed
            if (now - pr.date).days > num_days.days:
                logging.info("PUT: deleting PUT request {}".format(pr.pk))
                pr.delete()
        except Exception as e:
            logging.error("PUT: error in PUT_completed {}".format(str(e)))


def GET_completed(backend_object, config):
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
    now = datetime.datetime.utcnow()
    num_days = datetime.timedelta(days=config["COMPLETED_REQUEST_DAYS"])
    for gr in get_reqs:
        try:
            if gr.locked:
                continue
            # remove the request if the requisite time has elapsed
            if (now - gr.date).days > num_days.days:
                logging.info("GET: deleting GET request {}".format(gr.pk))
                gr.delete()
        except Exception as e:
            logging.error("GET: error in GET_completed {}".format(str(e)))


def DELETE_completed(backend_object, config):
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
    now = datetime.datetime.utcnow()
    num_days = datetime.timedelta(days=config["COMPLETED_REQUEST_DAYS"])
    for dr in del_reqs:
        try:
            if dr.locked:
                continue
            # remove the request if the requisite time has elapsed
            if (now - dr.date).days > num_days.days:
                # get the associated PUT or MIGRATE requests - there should only
                # be one
                other_reqs = MigrationRequest.objects.filter(
                    (Q(request_type=MigrationRequest.PUT)
                    | Q(request_type=MigrationRequest.MIGRATE)
                    | Q(request_type=MigrationRequest.GET))
                    & Q(migration=dr.migration)
                    & Q(migration__storage__storage=storage_id)
                )

                for otr in other_reqs:
                    logging.info((
                        "DELETE: deleting request {} associated with DELETE request {}."
                    ).format(otr.pk, dr.pk))
                    otr.delete()

                logging.info("DELETE: deleting DELETE request {}".format(dr.pk))
                dr.delete()
                # delete the migration
                logging.info((
                    "DELETE: deleting migration {} associated with DELETE request {}."
                ).format(dr.migration.pk, dr.pk))
                dr.migration.delete()
                # we are done!
        except Exception as e:
            logging.error("DELETE: error in DELETE_completed {}".format(str(e)))


def FAILED_completed(backend_object, config):
    """Do the tasks for a FAILED request:
       unlock the original files
    """
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # these occur during a PUT or MIGRATE request
    fail_reqs = MigrationRequest.objects.filter(
       	(Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.FAILED)
        & Q(migration__storage__storage=storage_id)
	)
    for fr in fail_reqs:
        try:
            # we want to use the locked FAILED requests to restore the original
            # permissions on the files
            if fr.locked:
                unlock_original_files(backend_object, fr, config)
                fr.unlock()
                # transition to FAILED_COMPLETED
                fr.stage = MigrationRequest.FAILED_COMPLETED
                fr.save()

                # log
                logging.info((
                    "Transition: request ID: {} external ID: {}: FAILED->FAILED_COMPLETED"
                ).format(fr.pk, fr.migration.external_id))
        except Exception as e:
            logging.error("FAILED: error in FAILED_completed {}".format(str(e)))


def process(backend, config):
    backend_object = backend()
    # run in this order so that MigrationRequests are not deleted immediately
    # which aids debugging!
    PUT_completed(backend_object, config)
    GET_completed(backend_object, config)
    DELETE_completed(backend_object, config)

    PUT_tidy(backend_object, config)
    GET_tidy(backend_object, config)
    DELETE_tidy(backend_object, config)

    FAILED_completed(backend_object, config)


def exit_handler(signal, frame):
    logging.info("Stopping jdma_tidy")
    sys.exit(0)


def run_loop(backend, config):
    # moved this to a function so we can call a one-shot version
    if backend is None:
        for backend in jdma_control.backends.get_backends():
            process(backend, config)
    else:
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend, config)


def run(*args):
    # setup the logging
    config = read_process_config("jdma_tidy")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_tidy")

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
            run_loop(backend, config)
            sleep(5)
    else:
        run_loop(backend, config)
