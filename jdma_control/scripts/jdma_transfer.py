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

from django.db.models import Q

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.models import StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging, calculate_digest
import jdma_control.backends
import jdma_control.backends.AES_tools as AES_tools


def mark_migration_failed(mig_req, failure_reason, upload_mig=True):
    logging.error(failure_reason)
    mig_req.stage = MigrationRequest.FAILED
    mig_req.failure_reason = failure_reason
    # only reset these if the upload migration (PUT | MIGRATE) fails
    # if a GET fails then the migration is unaffected
    if upload_mig:
        mig_req.migration.stage = Migration.FAILED
        #mig_req.migration.external_id = None
        mig_req.migration.save()
    mig_req.save()


def create_upload_batch(backend_object, credentials, pr):
    # check we actually have some files to archive first
    if pr.migration.migrationarchive_set.all().count() != 0:
        # open a connection to the backend.  Creating the connection can account
        # for a significant portion of the run time.  So we only do it once!
        conn = backend_object.create_connection(
            pr.migration.user.name,
            pr.migration.workspace.workspace,
            credentials
        )
        # Use the Backend stored in settings.JDMA_BACKEND_OBJECT to create the
        # batch
        try:
            external_id = backend_object.create_upload_batch(conn)
        except Exception as e:
            storage_name = StorageQuota.get_storage_name(
                pr.migration.storage.storage
            )
            error_string = (
                "Failed to create the upload batch for migration: {} "
                "on external storage: {}. Exception: "
            ).format(pr.migration.pk, storage_name, e)
            raise Exception(error_string)
        else:
            pr.migration.external_id = external_id
            pr.migration.stage = Migration.PUTTING
            pr.stage = MigrationRequest.PUT_PACKING
            pr.migration.save()
            pr.save()
            logging.info((
                "Transition: batch ID: {} PUT_PENDING->PUT_PACKING"
            ).format(pr.migration.external_id))
    else:
        error_string = (
            "No files in PUT or MIGRATE request: {} PUT_PENDING->FAILED"
        ).format(pr.migration.formatted_filelist()[0] + "...")
        raise Exception(error_string)
        # close the connection
        backend_object.close_connection(conn)
    return external_id


def upload_batch(backend_object, credentials, pr):
    """Upload the batch, taking turns to upload a single archive at once."""
    # the batch has been created, the archives have been created (in jdma_pack)
    # upload the archives to the external storage
    # open a connection to the backend.  Creating the connection can account
    # for a significant portion of the run time.  So we only do it once for each
    # migration request!
    conn = backend_object.create_connection(
        pr.migration.user.name,
        pr.migration.workspace.workspace,
        credentials
    )

    st_arch = pr.last_archive
    n_arch = pr.migration.migrationarchive_set.count()
    # create the directory path for the batch (external_id)
    archive_path = os.path.join(
        backend_object.ARCHIVE_STAGING_DIR,
        pr.migration.external_id
    )
    # get the archive set here as it might change if we get it in the loop
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # upload
        try:
            # log message
            logging.info((
                "Uploading file: {} to {}"
            ).format(archive_path, backend_object.get_name()))
            # create the tar file path
            tar_file_path = os.path.join(archive_path, archive.get_id()) + ".tar"

            # upload
            backend_object.put(
                conn,
                pr.migration.external_id,
                tar_file_path,
            )
            # add one to last archive
            pr.last_archive += 1
            pr.save()
        except Exception as e:
            raise Exception(e)

    # close the batch on the external storage - for ET this will trigger the
    # transport
    backend_object.close_upload_batch(
        conn,
        pr.migration.external_id
    )
    # close the connection to the backend
    backend_object.close_connection(conn)

    # monitoring is handled by jdma_monitor, which will transition
    # PUTTING->VERIFY_PENDING when the batch has finished uploading

def start_verify(backend_object, pr):
    """Start the verification process.  Transition from
    VERIFY_PENDING->VERIFY_GETTING and create the target directory."""
    try:
        # get the name of the target directory
        target_dir = os.path.join(
            backend_object.VERIFY_DIR,
            "verify_{}".format(pr.migration.external_id)
        )
        # create the target directory if it doesn't exist
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        pr.stage = MigrationRequest.VERIFY_GETTING
        pr.save()
        logging.info((
            "Transition: batch ID: {} VERIFY_PENDING->VERIFY_GETTING"
        ).format(pr.migration.external_id))
    except Exception as e:
        raise(e)


def download_to_verify(backend_object, credentials, pr):
    """Download the archives files in the PUT request to the VERIFY_DIR."""
    # open a connection to the backend.  Creating the connection can account
    # for a significant portion of the run time.  So we only do it once!
    conn = backend_object.create_connection(
        pr.migration.user.name,
        pr.migration.workspace.workspace,
        credentials
    )

    # start at the last_archive so that interrupted uploads can be resumed
    st_arch = pr.last_archive
    n_arch = pr.migration.migrationarchive_set.count()
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        try:
            # get the name of the target directory
            verify_dir = os.path.join(
                backend_object.VERIFY_DIR,
                "verify_{}".format(pr.migration.external_id)
            )
            # use Backend.get to pull back the files to a temporary directory
            logging.info((
                "Downloading for verify: {}"
            ).format(pr.migration.external_id))

            # get the object name and download
            archive_name = archive.get_id() + ".tar"
            backend_object.get(
                conn,
                pr.migration.external_id,
                archive_name,
                verify_dir
            )
            # update the last good archive
            pr.last_archive += 1
            pr.save()
        except Exception as e:
            raise(e)
    # jdma_monitor will determine when the batch has finished downloading for
    # verification and transition VERIFY_GETTING->VERIFYING


def start_download(backend_object, gr):
    """Start the download process.  Transition from
    GET_PENDING->GETTING and create the target directory."""
    try:
        # get the name of the target directory
        target_dir = os.path.join(
            backend_object.ARCHIVE_STAGING_DIR,
            "{}".format(gr.migration.external_id)
        )
        # create the target directory if it doesn't exist
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        gr.stage = MigrationRequest.GETTING
        gr.save()
        logging.info((
            "Transition: batch ID: {} GET_PENDING->GETTING"
        ).format(gr.migration.external_id))

    except Exception as e:
        raise(e)


def download_to_staging_directory(backend_object, credentials, pr):
    """Download the archives files in the GET request to the STAGING_DIR."""
    # open a connection to the backend.  Creating the connection can account
    # for a significant portion of the run time.  So we only do it once!
    conn = backend_object.create_connection(
        pr.migration.user.name,
        pr.migration.workspace.workspace,
        credentials
    )

    # start at the last_archive so that interrupted uploads can be resumed
    st_arch = pr.last_archive
    n_arch = pr.migration.migrationarchive_set.count()
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to download and stage (tar)
        archive = archive_set[arch_num]
        try:
            # get the name of the target directory
            staging_dir = os.path.join(
                backend_object.ARCHIVE_STAGING_DIR,
                "{}".format(pr.migration.external_id)
            )
            # use Backend.get to pull back the files to a temporary directory
            logging.info((
                "Downloading for unarchiving: {}"
            ).format(pr.migration.external_id))

            # get the object name and download
            archive_name = archive.get_id() + ".tar"
            backend_object.get(
                conn,
                pr.migration.external_id,
                archive_name,
                staging_dir
            )
            # update the last good archive
            pr.last_archive += 1
            pr.save()
        except Exception as e:
            raise(e)

        # the archive has been dowloaded to the staging directory
        # now unarchive it to the target directory
        try:
            # get the path of the staged tarfile
            tarfile_path = os.path.join(staging_dir, archive_name)
            # open the tarfile in read only
            tar_file = TarFile(tarfile_path, mode='r')
            # untar each file
            for tar_info in tar_file.getmembers():
                tar_file.extract(tar_info, path=pr.target_path)
                logging.info((
                    "    Extracting file: {} from staged archive: {} to"
                    " directory: {}"
                ).format(tar_info.name, tarfile_path, pr.target_path))
            tar_file.close()

        except Exception as e:
            # error_string = (
            #     "Could not add file: {} to archive: {}"
            # ).format(mf.path, stage_path)
            # logging.error(error_string)
            raise Exception(e)


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
    for pr in put_reqs:
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
                create_upload_batch(backend_object, credentials, pr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e))
        # between these stages PUT_PACKING occurs in jdma_pack
        elif pr.stage == MigrationRequest.PUTTING:
        # in the process of putting
            try:
                upload_batch(backend_object, credentials, pr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e))

        # check if data is now on external storage and should be pulled
        # back for verification
        elif pr.stage == MigrationRequest.VERIFY_PENDING:
            try:
                start_verify(backend_object, pr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e))

        # pull back the data from the external storage
        elif pr. stage == MigrationRequest.VERIFY_GETTING:
            try:
                download_to_verify(backend_object, credentials, pr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(pr, str(e))


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
            if not ("uidNumber" in q):
                error_string = ((
                    "uidNumber not in returned LDAP query for user id {}"
                ).format(mig_file.unix_user_id))
                raise Exception(error_string)
            else:
                uidNumber = q["uidNumber"][0]

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
            if not ("gidNumber" in q):
                error_string = ((
                    "gidNumber not in returned LDAP query for group id {}"
                ).format(mig_file.unix_group_id))
                raise Exception(error_string)
            else:
                gidNumber = q["gidNumber"][0]

            # form the file path
            file_path = os.path.join(
                gr.target_path,
                mig_file.path
            )

            # change the directory owner / group
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chown", "-R",
                 str(uidNumber)+":"+str(gidNumber),
                 file_path]
            )

            # change the permissions back to the original
            subprocess.call(
                ["/usr/bin/sudo",
                 "/bin/chmod", "-R",
                 str(mig_file.unix_permission),
                 file_path]
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
    conn = Connection.create(ldap_servers)

    # for each GET request get the Migration and determine if the type of the
    # Migration is GET_PENDING
    for gr in get_reqs:
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
                start_download(backend_object, gr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(gr, str(e), upload_mig=False)

        elif gr.stage == MigrationRequest.GETTING:
            # pull back the data from the backend
            try:
                download_to_staging_directory(backend_object, credentials, gr)
            except Exception as e:
                # Something went wrong, set FAILED and failure_reason
                mark_migration_failed(gr, str(e), upload_mig=False)

        elif gr.stage == MigrationRequest.GET_RESTORE:
            # restore the file permissions
            try:
                restore_owner_and_group(backend_object, gr, conn)
            except Exception as e:
                mark_migration_failed(gr, str(e), upload_mig=False)
    conn.close()

def run():
    # setup the logging
    setup_logging(__name__)
    # read the decrypt key
    key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)
    # loop over all backends - should we parallelise these?
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        put_transfers(backend_object, key)
        get_transfers(backend_object, key)
