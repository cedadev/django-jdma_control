"""Functions to transfer the files in a request to (PUT) / from (GET) the
   external storage.
   Running this will change the state of the migrations
   and will invoke functions on the backed for PUT / GET operations to
   external storage.  The external_id returned from Backend.CreateUploadBatch
   will be recorded in the Migration object
"""

import os
import logging
import subprocess
from tarfile import TarFile
import signal
import sys
from time import sleep

from django.db.models import Q

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest, MigrationArchive
from jdma_control.models import StorageQuota
import jdma_control.backends
import jdma_control.backends.AES_tools as AES_tools
from jdma_control.scripts.common import mark_migration_failed, setup_logging
from jdma_control.scripts.common import calculate_digest
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import get_verify_dir, get_staging_dir, get_download_dir
from jdma_control.backends.ConnectionPool import ConnectionPool

connection_pool = ConnectionPool()

def upload(backend_object, credentials, pr):
    # check we actually have some files to archive first
    if pr.migration.migrationarchive_set.all().count() == 0:
        error_string = (
            "No files in PUT or MIGRATE request: {} PUT_PENDING->FAILED"
        ).format(pr.migration.formatted_filelist()[0] + "...")
        raise Exception(error_string)
    else:
        try:
            # open a connection to the backend.  Creating the connection can account
            # for a significant portion of the run time.  So we only do it once!
            global connection_pool
            conn = connection_pool.find_or_create_connection(
                backend_object,
                pr,
                credentials,
                mode="upload",
                uid = "PUT"
            )
            # There are two types of backends - piecewise and non-piecewise
            # piecewise (ObjectStore, FTP) allows you to upload archive by
            # archive and resume uploads.
            # Non-piecewise (ElasticTape) does not allow to upload archive by
            # archive and requires all of the files to be uploaded at once.
            # Non-piecewise also does not support resumin interrupted uploads.

            # transition the stage from PUT_PACKING->PUTTING
            pr.migration.stage = Migration.PUTTING
            pr.stage = MigrationRequest.PUTTING
            pr.migration.save()
            pr.save()
            logging.info((
                "Transition: batch ID: {} PUT_PACKING->PUTTING"
            ).format(pr.migration.external_id))

            # get the archive set here as it might change if we get it in the loop
            archive_set = pr.migration.migrationarchive_set.order_by('pk')

            if backend_object.piecewise():
                st_arch = pr.last_archive
                n_arch = pr.migration.migrationarchive_set.count()
                # loop over the archives
                for arch_num in range(st_arch, n_arch):
                    # get the archive from the archive number
                    archive = archive_set[arch_num]
                    # check whether the archive is packed
                    if archive.packed:
                        prefix = get_staging_dir(backend_object, pr)
                    else:
                        prefix = pr.migration.common_path
                    # get the list of files for this archive
                    file_list = archive.get_file_names(
                        prefix,
                        directories = False
                    )
                    # log message
                    logging.info((
                        "Uploading files: {} to {}"
                    ).format(file_list, backend_object.get_name()))
                    # upload objects in filelist
                    file_inc = backend_object.upload_files(
                        conn,
                        pr,
                        prefix,
                        file_list
                    )
                    # inc archive if all files went up
                    if int(file_inc == len(file_list)):
                        pr.last_archive += 1
                        pr.save()
            else:
                # get the archive set - add all files to the filelist
                file_list = []
                for archive in archive_set:
                    # get a list of files, using the relevant prefix
                    if archive.packed:
                        # concat the directory path for the batch (internal_id)
                        prefix = get_staging_dir(backend_object, pr)
                    else:
                        prefix = pr.migration.common_path
                    # add files in this archive to those already added
                    archive_files = archive.get_file_names(
                        prefix,
                        directories=False
                    )
                    file_list.extend(archive_files)
                # log message
                logging.info((
                    "Uploading files: {} to {}"
                ).format(file_list, backend_object.get_name()))
                # Upload filelist
                file_inc = backend_object.upload_files(
                    conn,
                    pr,
                    prefix,
                    file_list
                )
                # inc archive if all files went up
                if int(file_inc == len(file_list)):
                    pr.last_archive += 1
                    pr.save()

            # close the connection in the pool
            conn = connection_pool.close_connection(
                backend_object,
                pr,
                credentials,
                mode="upload",
                uid = "PUT"
            )

        except Exception as e:
            storage_name = StorageQuota.get_storage_name(
                pr.migration.storage.storage
            )
            error_string = (
                "Failed to create the upload batch for migration: {} "
                "on external storage: {}. Exception: {}"
            ).format(pr.migration.pk, storage_name, str(e))
            raise Exception(error_string)


def verify(backend_object, credentials, pr):
    """Start the verification process.  Transition from
    VERIFY_PENDING->VERIFY_GETTING and create the target directory.
    Download the batch from the backend storage to the target directory
    """
    try:
        # open a connection to the backend.  Creating the connection can account
        # for a significant portion of the run time.  So we only do it once!
        global connection_pool
        conn = connection_pool.find_or_create_connection(
            backend_object,
            pr,
            credentials,
            mode="download",
            uid="VERIFY"
        )
        # Transition
        pr.stage = MigrationRequest.VERIFY_GETTING
        pr.save()
        logging.info((
            "Transition: batch ID: {} VERIFY_PENDING->VERIFY_GETTING"
        ).format(pr.migration.external_id))

        # get the name of the verification directory
        target_dir = get_verify_dir(backend_object, pr)
        # create the target directory if it doesn't exist
        try:
            os.makedirs(target_dir)
        except:
            pass

        # for verify, we want to get the whole batch
        # get the archive set
        archive_set = pr.migration.migrationarchive_set.order_by('pk')

        # add all the files in the archive to a file_list for downloading
        file_list = []
        for archive in archive_set:
            # add files in this archive to those already added
            archive_files = archive.get_filtered_file_names(prefix="")
            file_list.extend(archive_files)

        backend_object.download_files(
            conn,
            pr,
            file_list = file_list,
            target_dir = target_dir
        )
        conn = connection_pool.close_connection(
            backend_object,
            pr,
            credentials,
            mode="download",
            uid="VERIFY"
        )
    except Exception as e:
        storage_name = StorageQuota.get_storage_name(
            pr.migration.storage.storage
        )
        error_string = (
            "Failed to download for verify the migration: {} "
            "on external storage: {}. Exception: {}"
        ).format(pr.migration.pk, storage_name, str(e))
        raise Exception(error_string)


def download(backend_object, credentials, gr):
    """Start the download process.  Transition from
    GET_PENDING->GETTING and create the target directory."""

    try:
        global connection_pool
        conn = connection_pool.find_or_create_connection(
            backend_object,
            gr,
            credentials,
            mode="download",
            uid="GET"
        )
        # Transition
        gr.stage = MigrationRequest.GETTING
        gr.save()
        logging.info((
            "Transition: batch ID: {} GET_PENDING->GETTING"
        ).format(gr.migration.external_id))

        # we just (potentially) want to get a subset of archives
        archive_set, st_arch, n_arch = get_archive_set_from_get_request(gr)
        # empty file list
        file_list = []

        for arch_num in range(st_arch, n_arch):
            # determine which archive to download and stage (tar)
            archive = archive_set[arch_num]
            # get the name of the target directory
            # if the archive is packed then the name is the download directory
            # (this code currently assumes that all archives are either packed
            # or not.  This could change with some adaptation here)
            if archive.packed:
                target_dir = get_download_dir(backend_object, gr)
            else:
                # if not packed then it is the target directory from the request
                target_dir = gr.target_path

            # create the target directory if it doesn't exist
            try:
                os.makedirs(target_dir)
            except:
                pass

            # get the filelist - filter on digest and whether the file
            # has been requested
            filt_file_list = archive.get_filtered_file_names(
                filelist=gr.filelist
            )
            # if piecewise then download bit by bit, otherwise add to file_list
            # and download at the end
            if backend_object.piecewise():
                backend_object.download_files(
                    conn,
                    gr,
                    file_list = filt_file_list,
                    target_dir = target_dir
                )
            else:
                file_list.extend(filt_file_list)
        # Download all if not piecewise
        if not backend_object.piecewise():
            backend_object.download(
                conn,
                gr,
                file_list = file_list,
                target_dir = target_dir
            )
        # close the connection
        conn = connection_pool.close_connection(
            backend_object,
            gr,
            credentials,
            mode="download",
            uid="GET"
        )

    except Exception as e:
        storage_name = StorageQuota.get_storage_name(
            gr.migration.storage.storage
        )
        error_string = (
            "Failed to download the migration: {} "
            "on external storage: {}. Exception: {}"
        ).format(gr.migration.pk, storage_name, str(e))
        raise Exception(error_string)


def put_transfers(backend_object, key):
    """Work through the state machine to upload batches to the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend.
    # This involves resolving two foreign keys
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage__in=[
            MigrationRequest.PUT_PENDING,
            MigrationRequest.PUTTING,
            MigrationRequest.VERIFY_PENDING,
            MigrationRequest.VERIFY_GETTING
        ])
    )
    # for each PUT request get the Migration and determine the type of the
    # Migration
    put_count = 0
    for pr in put_reqs:
        # check for lock
        if pr.locked:
            continue
        # lock
        pr.lock()
        # determine the credentials for the user - decrypt if necessary
        if pr.credentials != {}:
            credentials = AES_tools.AES_decrypt_dict(key, pr.credentials)
        else:
            credentials = {}

        # Check whether data is being put to external storage
        if pr.stage == MigrationRequest.PUT_PENDING:
            # create the batch on this instance, next time the script is run
            # the archives will be created as tarfiles
            try:
                upload(backend_object, credentials, pr)
                put_count += 1
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e), e)
        # between these stages PUT_PACKING occurs in jdma_pack
        elif pr.stage == MigrationRequest.PUTTING:
            # in the process of putting - we don't do anything here now!
            # jdma_monitor script will transition from PUTTING->VERIFY_PENDING
            pass
        # check if data is now on external storage and should be pulled
        # back for verification
        elif pr.stage == MigrationRequest.VERIFY_PENDING:
            # pull back the data from the external storage
            try:
                put_count += 1
                verify(backend_object, credentials, pr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e), e)

        elif pr.stage == MigrationRequest.VERIFY_GETTING:
            pass
        elif pr.stage == MigrationRequest.VERIFYING:
            pass
        # unlock
        pr.unlock()
    return put_count


def restore_owner_and_group(backend_object, gr, conn):
    # change the owner, group and permissions of the file to match that
    # of the original from the user query

    # start at the last_archive so that interrupted uploads can be resumed
    st_arch = gr.last_archive
    n_arch = gr.migration.migrationarchive_set.count()
    archive_set = gr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to change the permissions for
        archive = archive_set[arch_num]

        # get the migration files in the archive
        mig_files = archive.migrationfile_set.all()
        for mig_file in mig_files:
            # query for the user
            query = Query(
                conn,
                base_dn=settings.JDMA_LDAP_BASE_USER
            ).filter(uid=mig_file.unix_user_id)
            # check for a valid return
            if len(query) == 0:
                error_string = ((
                    "Unix user id: {} not found from LDAP in monitor_get"
                ).format(mig_file.unix_user_id))
                raise Exception(error_string)
            # use just the first returned result
            q = query[0]
            # # check that the keys exist in q
            try:
                uidNumber = q["uidNumber"][0]
            except:
                error_string = ((
                    "uidNumber not in returned LDAP query for user id {}"
                ).format(mig_file.unix_user_id))
                raise Exception(error_string)

            # query for the group
            query = Query(
                conn,
                base_dn=settings.JDMA_LDAP_BASE_GROUP
            ).filter(cn=mig_file.unix_group_id)
            # check for a valid return
            if len(query) == 0:
                error_string = ((
                    "Unix group id: {} not found from LDAP in monitor_get"
                ).format(mig_file.unix_group_id))
                raise Exception(error_string)
            # use just the first returned result
            q = query[0]
            # check that the keys exist in q
            try:
                gidNumber = q["gidNumber"][0]
            except:
                error_string = ((
                    "gidNumber not in returned LDAP query for group id {}"
                ).format(mig_file.unix_group_id))
                raise Exception(error_string)

            # form the file path
            file_path = os.path.join(
                gr.target_path,
                mig_file.path
            )

            # Note that, if there is a filelist then the files may not exist
            # However, to successfully restore any parent directories that may
            # be created, we have to attempt to restore all of the files in the
            # archive.  We'll do this by checking the filepath
            if os.path.exists(file_path):
                # change the directory owner / group
                subprocess.call(
                    ["/usr/bin/sudo",
                     "/bin/chown",
                     str(uidNumber)+":"+str(gidNumber),
                     file_path]
                )

                # change the permissions back to the original
                subprocess.call(
                    ["/usr/bin/sudo",
                     "/bin/chmod",
                     str(mig_file.unix_permission),
                     file_path]
                )
    # restore the target_path
    # change the directory owner / group
    subprocess.call(
        ["/usr/bin/sudo",
         "/bin/chown",
         str(gr.migration.common_path_user_id)+":"+str(gr.migration.common_path_group_id),
         gr.target_path]
    )

    # change the permissions back to the original
    subprocess.call(
        ["/usr/bin/sudo",
         "/bin/chmod",
         str(gr.migration.common_path_permission),
         gr.target_path]
    )


    # if we reach this point then the restoration has finished.
    # next stage is tidy up
    gr.stage = MigrationRequest.GET_TIDY
    gr.last_archive = 0
    gr.save()


def get_transfers(backend_object, key):
    """Work through the state machine to download batches from the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # get the GET requests which are queued (GET_PENDING) for this backend
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
    )
    # create the required ldap server pool, do this just once to
    # improve performance
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                              settings.JDMA_LDAP_REPLICAS)
    ldap_conn = Connection.create(ldap_servers)

    # for each GET request get the Migration and determine if the type of the
    # Migration is GET_PENDING
    get_count = 0
    for gr in get_reqs:
        # check for lock
        if gr.locked:
            continue
        gr.lock()
        # determine the credentials for the user - decrypt if necessary
        if gr.credentials != {}:
            credentials = AES_tools.AES_decrypt_dict(key, gr.credentials)
        else:
            credentials = {}

        if gr.stage == MigrationRequest.GET_PENDING:
            # we might have to do something here, like create a download batch
            # for elastic tape.  Also create the directory and transition the
            # state
            try:
                download(backend_object, credentials, gr)
                get_count += 1
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(gr, str(e), e, upload_mig=False)

        elif gr.stage == MigrationRequest.GETTING:
            pass

        elif gr.stage == MigrationRequest.GET_RESTORE:
            # restore the file permissions
            try:
                get_count += 1
                restore_owner_and_group(backend_object, gr, ldap_conn)
            except Exception as e:
                mark_migration_failed(gr, str(e), e, upload_mig=False)
        gr.unlock()
    ldap_conn.close()
    return get_count


def start_delete(backend_object, credentials, dr):
    """Create a delete batch on the external storage.
    Set the last archive to 0.
    Transition to DELETING."""
    global connection_pool
    conn = connection_pool.find_or_create_connection(
        backend_object,
        dr,
        credentials,
        mode="delete",
        uid="DELETE"
    )
    dr.migration.last_archive = 0
    dr.migration.save()
    dr.stage = MigrationRequest.DELETING
    dr.save()


def delete_batch(backend_object, credentials, dr):
    """Delete the batch, taking turns to delete a single archive at once."""
    # open a connection to the backend.  Creating the connection can account
    # for a significant portion of the run time.  So we only do it once!
    global connection_pool
    conn = connection_pool.find_or_create_connection(
        backend_object,
        dr,
        credentials,
        mode="delete",
        uid="DELETE"
    )
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # find the associated PUT or MIGRATE migration request:
    # the number of archives uploaded comes from the last_archive of this
    # migration request (if not zero)
    try:
        put_req = MigrationRequest.objects.get(
            (Q(request_type=MigrationRequest.PUT) |
            Q(request_type=MigrationRequest.MIGRATE))
            & Q(migration=dr.migration)
            & Q(migration__storage__storage=storage_id)
        )
    except:
        put_req = None
    # start at the last_archive so that interrupted deletes can be resumed
    st_arch = dr.last_archive
    # determine how many archives have actually been uploaded
    if put_req and put_req.last_archive != 0:
        n_arch = put_req.last_archive
    else:
        n_arch = dr.migration.migrationarchive_set.count()
    # loop over the number of archives
    archive_set = dr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to delete
        archive = archive_set[arch_num]
        try:
            logging.info((
                "Deleting: {}/{}"
            ).format(dr.migration.external_id, archive.get_id()))
            backend_object.delete(
                conn,
                dr.migration.external_id,
                archive.get_id(),
            )
            # update the last good archive
            dr.last_archive += 1
            dr.save()

        except Exception as e:
            raise(e)

    # close the batch on the external storage - for ET this will trigger the
    # transport
    backend_object.close_delete_batch(
        conn,
        dr.migration.external_id
    )


def delete_transfers(backend_object, key):
    """Work through the state machine to delete batches from the external
    storage"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # get the GET requests which are queued (GET_PENDING) for this backend
    del_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.DELETE)
        & Q(migration__storage__storage=storage_id)
    )
    # create the required ldap server pool, do this just once to
    # improve performance
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                              settings.JDMA_LDAP_REPLICAS)
    ldap_conn = Connection.create(ldap_servers)

    # for each GET request get the Migration and determine if the type of the
    # Migration is GET_PENDING
    del_count = 0
    for dr in del_reqs:
        # check for lock
        if dr.locked:
            continue
        dr.lock()

        # find the associated PUT or MIGRATE migration request
        # if there is one - if not, set put_req to None
        # there will not be a migration request if the migration has completed
        # as the migration request is deleted when a PUT or MIGRATE completes
        try:
            put_req = MigrationRequest.objects.get(
                (Q(request_type=MigrationRequest.PUT) |
                 Q(request_type=MigrationRequest.MIGRATE))
                & Q(migration=dr.migration)
                & Q(migration__storage__storage=storage_id)
            )
        except:
            put_req = None

        # determine the credentials for the user - decrypt if necessary
        if dr.credentials != {}:
            credentials = AES_tools.AES_decrypt_dict(key, dr.credentials)
        else:
            credentials = {}

        # switch on the state machine status
        if dr.stage == MigrationRequest.DELETE_PENDING:
            try:
                # only try to do the delete if some files have been uploaded!
                if ((put_req and put_req.stage > MigrationRequest.PUT_PACKING)
                   or (dr.migration.stage == Migration.ON_STORAGE)
                ):
                    start_delete(backend_object, credentials, dr)
                else:
                # transition to DELETE_TIDY if there are no files to delete
                    dr.stage = MigrationRequest.DELETE_TIDY
                    dr.save()
                del_count += 1
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(dr, str(e), e, upload_mig=False)

        elif dr.stage == MigrationRequest.DELETING:
        # in the process of deleting
            try:
                delete_batch(backend_object, credentials, dr)
                del_count += 1
            except Exception as e:
                mark_migration_failed(dr, str(e), e)
        # unlock
        dr.unlock()
    ldap_conn.close()
    return del_count


def process(backend, key):
    """Run the transfer processes on a backend.
    Keep a running total of whether any processes were run.
    If they weren't then put the daemon to sleep for a minute to prevent the
    database being hammerred"""
    backend_object = backend()

    n_put = put_transfers(backend_object, key)
    n_get = get_transfers(backend_object, key)
    n_del = delete_transfers(backend_object, key)
    return n_put + n_get + n_del


def exit_handler(signal, frame):
    global connection_pool
    connection_pool.close_all_connections()
    sys.exit(0)


def run(*args):
    # setup the logging
    setup_logging(__name__)
    # setup exit signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)
    global connection_pool

    # run this indefinitely until the signals are triggered
    while True:
        #try:
        if True:
            # read the decrypt key
            key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)
            n_procs = 0
            # run as a daemon
            if len(args) == 0:
                for backend in jdma_control.backends.get_backends():
                    n_procs += process(backend, key)
            else:
                backend = args[0]
                if not backend in jdma_control.backends.get_backend_ids():
                    logging.error("Backend: " + backend + " not recognised.")
                else:
                    backend = jdma_control.backends.get_backend_from_id(backend)
                    n_procs += process(backend, key)

            # print the number of connections
            sum_c = 0
            for c in connection_pool.pool:
                sum_c += len(connection_pool.pool)
            # sleep for ten secs if nothing happened in the loop
            if n_procs == 0:
                sleep(10)
        else:
        #except Exception as e:
            # catch all exceptions as we want this to run in a loop for all
            # backends and transfers - we don't want one transfer to crash out
            # the transfer daemon with a single bad transfer!
            # output the exception to the log so we can see what went wrong
            logging.error(str(e))
        # print ("Number of connections: {}".format(sum_c))
        #
