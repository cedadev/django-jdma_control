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
import jdma_site.settings as settings

def get_completed_puts():
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
                # form the object name
                object_name = archive.get_id() + ".tar"
                # use head_object to check if the object is written
                if s3c.head_object(Bucket=pr.migration.external_id,
                                   Key=object_name):
                    n_up_arch += 1
            if n_up_arch == len(archive_set):
                completed_PUTs.append(pr.migration.external_id)
        except Exception as e:
            raise Exception(e)

    return completed_PUTs


def get_completed_gets():
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, StorageQuota
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
        # get the name of the target directory (same for each archive in a
        # migration)
        verify_dir = os.path.join(
            OS_Settings.VERIFY_DIR,
            "verify_{}".format(gr.migration.external_id)
        )
        n_completed_archives = 0
        # loop over each archive in the migration
        archive_set = gr.migration.migrationarchive_set.order_by('pk')
        # just need to see if the archive has been downloaded to the file system
        # we know this when the file is present and the file size is equal to
        # that stored in the database
        for archive in archive_set:
            # form the filepath
            archive_name = archive.get_id() + ".tar"
            tar_file_path = os.path.join(verify_dir, archive_name)
            # check for existance first
            if os.path.exists(tar_file_path):
                # now check for size
                size = os.stat(tar_file_path).st_size
                if size == archive.size:
                    n_completed_archives += 1
        # if number completed is equal to number in archive set then the
        # transfer has completed
        if n_completed_archives == len(archive_set):
            completed_GETs.append(gr.migration.external_id)
    return completed_GETs


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
            completed_PUTs = get_completed_puts()
            completed_GETs = get_completed_gets()
        except Exception as e:
            raise Exception(e)
        return completed_PUTs, completed_GETs

    def create_connection(self, user, workspace, credentials):
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

    def create_download_batch(self, conn):
        """Do nothing for object store."""
        return

    def close_download_batch(self, conn):
        """Do nothing for object store."""
        return

    def get(self, conn, batch_id, archive, target_dir):
        """Download a batch of files from the Object Store to a target
        directory.
        """
        # get the last part of the filepath
        object_name = os.path.basename(archive)
        download_file_path = os.path.join(target_dir, object_name)
        conn.download_file(batch_id, object_name, download_file_path)

    def create_upload_batch(self, conn):
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

    def put(self, conn, batch_id, archive):
        """Put a staged archive (with path archive) onto the Object Store"""
        # get the last part of the filepath
        object_name = os.path.basename(archive)
        conn.upload_file(archive, batch_id, object_name)

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

    def user_has_get_permission(self, conn):
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

    def user_has_put_quota(self, conn, filelist):
        """Get the remaining quota for the user in the workspace.
        How can we do this in Object Store backend?
        """
        return True

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
