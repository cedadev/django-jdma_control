"""Class for a JASMIN Data Migration App backend that targets an object store
   backend with S3 HTTP API.
   Uses boto3 API, but could easily be switched to use minio or another API.

   Creating a migration on an object store consists of the following
   operations:
    1.  Create a bucket for the group workspace and current batch id, as an
        identifier
    2.  Upload a tarfile archive to the bucket as part of the migrations

   """
import os

import boto3
from django.db.models import Q

from jdma_control.backends.Backend import Backend
from jdma_control.backends import ObjectStoreSettings as OS_Settings
from jdma_control.backends import AES_tools
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import get_verify_dir, get_staging_dir
import jdma_site.settings as settings

def get_completed_puts(backend_object):
    """Get all the completed puts for the ObjectStore"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("objectstore")
    # get the decrypt key
    key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)

    # list of completed PUTs to return
    completed_PUTs = []
    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT))
        & Q(stage=MigrationRequest.PUTTING)
        & Q(migration__stage=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # decrypt the credentials
        credentials = AES_tools.AES_decrypt_dict(key, pr.credentials)
        try:
            # create a connection to the object store
            s3c = boto3.client("s3", endpoint_url=OS_Settings.S3_ENDPOINT,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
            # loop over each archive in the migration
            archive_set = pr.migration.migrationarchive_set.order_by('pk')
            # counter for number of uploaded archives
            n_up_arch = 0
            for archive in archive_set:
                # get the list of files for this archive
                file_list = archive.get_filtered_file_names()
                n_files = 0
                for file_path in file_list:
                    # object name is the file_path, without any prefix
                    object_name = file_path
                    try:
                        if s3c.head_object(Bucket=pr.migration.external_id,
                                           Key=object_name):
                            n_files += 1
                    except:
                        pass
                # check if all files uploaded and then inc archive
                if n_files == len(file_list):
                    n_up_arch += 1
            if n_up_arch == pr.migration.migrationarchive_set.count():
                completed_PUTs.append(pr.migration.external_id)

        except Exception as e:
            raise Exception(e)

    return completed_PUTs


def get_completed_gets(backend_object):
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, StorageQuota
    from jdma_control.models import MigrationFile, MigrationArchive
    # get the storage id
    storage_id = StorageQuota.get_storage_index("objectstore")

    # list of completed GETs to return
    completed_GETs = []
    # now loop over the GET requests
    get_reqs = MigrationRequest.objects.filter(
        (Q(stage=MigrationRequest.GETTING)
        | Q(stage=MigrationRequest.VERIFY_GETTING))
        & Q(migration__storage__storage=storage_id)
    )
    #
    for gr in get_reqs:
        # loop over each archive in the migration
        # if the filelist for the GET request is not None then we have to determine
        # which archives to download
        archive_set, st_arch, n_arch = get_archive_set_from_get_request(gr)
        # just need to see if the archive has been downloaded to the file system
        # we know this when the file is present and the file size is equal to
        # that stored in the database
        n_completed_archives = 0
        for archive in archive_set:
            # Determine the staging directory.  Three options:
            # 1. The stage is VERIFY_GETTING->VERIFY DIR
            # 2. The stage is GETTING and archive.packed->STAGING_DIR
            # 3. The stage is GETTING and not archive.packed->target_path
            # form the filepath
            if gr.stage == MigrationRequest.VERIFY_GETTING:
                staging_dir = get_verify_dir(backend_object, gr)
            elif gr.stage == MigrationRequest.GETTING:
                if archive.packed:
                    staging_dir = get_staging_dir(backend_object, gr)
                else:
                    staging_dir = gr.target_path
            # now loop over each file in the archive
            n_completed_files = 0
            file_name_list = archive.get_filtered_file_names()
            for file_name in file_name_list:
                file_path = os.path.join(staging_dir, file_name)
                try:
                    # just rely on exception thown if file does not exist yet
                    # now check for size
                    size = os.stat(file_path).st_size
                    # for packed archive check the archive size
                    if archive.packed:
                        n_completed_files += int(size == archive.size)
                    else:
                        # get the file from the db
                        file_obj = MigrationFile.objects.get(
                            path=file_name,
                            archive=archive
                        )
                        n_completed_files += int(size == file_obj.size)
                except:
                    pass
            # add if all files downloaded from archive
            if n_completed_files == len(file_name_list):
                n_completed_archives += 1
        # if number completed is equal to number in archive set then the
        # transfer has completed
        if n_completed_archives == len(archive_set):
            completed_GETs.append(gr.transfer_id)
    return completed_GETs


def get_completed_deletes(backend_object):
    """Get all the completed deletes for the ObjectStore"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("objectstore")
    # get the decrypt key
    key = AES_tools.AES_read_key(settings.ENCRYPT_KEY_FILE)

    # list of completed DELETEs to return
    completed_DELETEs = []
    # now loop over the PUT requests
    del_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.DELETE))
        & Q(stage=MigrationRequest.DELETING)
        & Q(migration__storage__storage=storage_id)
    )
    for dr in del_reqs:
        # decrypt the credentials
        credentials = AES_tools.AES_decrypt_dict(key, dr.credentials)
        try:
            # create a connection to the object store
            s3c = boto3.client("s3", endpoint_url=OS_Settings.S3_ENDPOINT,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
            # if the bucket has been deleted then the deletion has completed
            buckets = s3c.list_buckets()
            if ('Buckets' not in buckets
                 or dr.migration.external_id not in buckets['Buckets']):
                completed_DELETEs.append(dr.migration.external_id)
        except Exception as e:
            raise Exception(e)
    return completed_DELETEs


class ObjectStoreBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets an Object
    Store with S3 HTTP API.
    Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.VERIFY_DIR = OS_Settings.VERIFY_DIR
        self.ARCHIVE_STAGING_DIR = OS_Settings.ARCHIVE_STAGING_DIR

    def available(self, credentials):
        """Return whether the object store is available or not"""
        try:
            s3c = boto3.client("s3", endpoint_url=OS_Settings.S3_ENDPOINT,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
            s3c.list_buckets()
            return True
        except Exception:
            return False

    def monitor(self):
        """Determine which batches have completed."""
        try:
            completed_PUTs = get_completed_puts(self)
            completed_GETs = get_completed_gets(self)
            completed_DELETEs = get_completed_deletes(self)
        except Exception as e:
            raise Exception(e)
        return completed_PUTs, completed_GETs, completed_DELETEs

    def pack_data(self):
        """Should the data be packed into a tarfile for this backend?"""
        return True

    def create_connection(self, user, workspace, credentials, mode="upload"):
        # create connection to Object Store, using the supplied credentials
        s3c = boto3.client("s3", endpoint_url=OS_Settings.S3_ENDPOINT,
                           aws_access_key_id=credentials['access_key'],
                           aws_secret_access_key=credentials['secret_key'])
        s3c.jdma_user = user
        s3c.jdma_workspace = workspace
        return s3c

    def close_connection(self, conn):
        """Close the connection to the backend.  Do nothing for the object store
        """
        return

    def create_download_batch(self, conn, get_req, file_list=[]):
        """Do nothing for object store."""
        return get_req.migration.external_id

    def close_download_batch(self, conn, transfer_id):
        """Do nothing for object store."""
        return

    def get(self, conn, get_req, object_name, target_dir):
        """Download a batch of files from the Object Store to a target
        directory.
        """
        download_file_path = os.path.join(target_dir, object_name)
        # check that the the sub path exists
        sub_path = os.path.split(download_file_path)[0]
        # The "it's better to ask forgiveness method!"
        try:
            os.makedirs(sub_path)
        except:
            pass
        conn.download_file(transfer_id, object_name, download_file_path)
        return 1

    def create_upload_batch(self, conn, put_req, file_list=[]):
        """Create a batch on the object store and return the batch id.
        For the object store the batch id is the groupworkspace name appended
        with the next batch number for that groupworkspace.
        """
        gws_bucket_prefix = "gws-" + conn.jdma_workspace + "-"
        # list all the buckets and filter those that contain the bucket prefix
        # find the highest number suffix
        try:
            response = conn.list_buckets()
            if len(response["Buckets"]) == 0:
                batch_id = 0
            else:
                batch_id = 0
                for bucket in response["Buckets"]:
                    # check that the gws_bucket_prefix is in bucket name
                    if gws_bucket_prefix in bucket["Name"]:
                        # get the id
                        c_id = int(bucket["Name"][len(gws_bucket_prefix):])
                        # check whether this is greatest batch id and create
                        # one larger if it is
                        if c_id >= batch_id:
                            batch_id = c_id + 1

            # create the bucket name: format c_id to 10 digits
            bucket_name = "{}{:010}".format(gws_bucket_prefix, batch_id)
            # create the bucket
            conn.create_bucket(Bucket=bucket_name)
            # need some ACL to control access to the bucket - limit to users in
            # the group workspace
            batch_id = bucket_name
        except Exception as e:
            batch_id = None
            raise Exception(str(e))

        return batch_id

    def close_upload_batch(self, conn, batch_id):
        """Close the batch on the external storage.
           Not needed for object store"""
        return

    def put(self, conn, put_req, archive_path, packed=False):
        """Put a staged archive (with path archive) onto the Object Store"""
        if packed:
            # get the last part of the path
            path_split = os.path.split(archive_path)
            object_name = path_split[-1]
        else:
            object_name = os.path.relpath(archive_path,
                                          put_req.migration.common_path)
        conn.upload_file(archive_path,
                         put_req.migration.external_id,
                         object_name)
        return 1

    def create_delete_batch(self, conn):
        """Do nothing on the object store"""
        return None

    def close_delete_batch(self, conn, batch_id):
        """Delete the bucket when the batch is deleted"""
        conn.delete_bucket(Bucket=batch_id)

    def delete(self, conn, batch_id, archive):
        """Delete a single tarred archive of files from the object store"""
        object_name = archive
        conn.delete_object(Bucket=batch_id, Key=object_name)

    def user_has_put_permission(self, conn):
        """Check whether the user has permission (via their access_key and
        secret_key) to access the object store, and whether they have
        permission from the groupworkspace
        """
        # groupworkspace permission
        gws_permission = Backend._user_has_put_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )

        # to validate the credentials we have to do some operation, as just
        # connecting the client doesn't do any validation!
        try:
            conn.list_buckets()
            s3_permission = True
        except Exception:
            s3_permission = False
        return gws_permission & s3_permission

    def user_has_get_permission(self, batch_id, conn):
        """Check whether the user has permission (via their access_key and
        secret_key) to access the object store, and whether they have
        permission from the groupworkspace
        """
        gws_permission = Backend._user_has_get_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )

        # to validate the credentials we have to do some operation, as just
        # connecting the client doesn't do any validation!
        try:
            conn.list_buckets()
            s3_permission = True
        except Exception:
            s3_permission = False
        return gws_permission & s3_permission

    def user_has_delete_permission(self, batch_id, conn):
        """Check whether the user has permission (via their access_key and
        secret_key) to delete the object from the object store, and whether they
        have permission from the groupworkspace LDAP.
        """
        # check from the groupworkspace
        gws_permission = Backend._user_has_delete_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace, batch_id
        )
        return gws_permission

    def user_has_put_quota(self, conn):
        """Check the remaining quota for the user in the workspace.
        We just check the database here, i.e. check that we are not over
        quota.
        When jdma_lock calculates the file sizes we can check the quota again
        and flag the transfer as FAILED if it goes over the quota.
        """
        from jdma_control.models import StorageQuota
        # get the storage id
        storage_id = StorageQuota.get_storage_index("objectstore")
        storage_quota = StorageQuota.objects.filter(
            storage=storage_id,
            workspace__workspace=conn.jdma_workspace
        )[0]
        return storage_quota.quota_used < storage_quota.quota_size

    def get_name(self):
        return "Object Store"

    def get_id(self):
        return "objectstore"

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
        These keys, along with their values, will be stored in a hidden file
        in the user's home directory.
        They will be encrypted and stored in the MigrationRequest so that
        the daemon processes can carry out the Migrations on behalf of the
        user.
        """
        return ["access_key", "secret_key"]

    def minimum_object_size(self):
        """Minimum recommend size for object store = 2GB? (check with Charles,
        Matt Jones, Jonathan Churchill, etc.)
        """
        return OS_Settings.OBJECT_SIZE
