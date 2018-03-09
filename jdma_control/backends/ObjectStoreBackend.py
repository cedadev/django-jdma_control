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

from jdma_control.backends.Backend import Backend
from jdma_control.backends import ObjectStoreSettings as OS_Settings


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
        completed_PUTs = ["2"]
        completed_GETs = []
        return completed_PUTs, completed_GETs

    def create_connection(self, user, workspace, credentials):
        # create connection to Object Store, using the supplied credentials
        s3c = boto3.client("s3", endpoint_url=OS_Settings.S3_ENDPOINT,
                           aws_access_key_id=credentials['access_key'],
                           aws_secret_access_key=credentials['secret_key'])
        return s3c

    def get(self, conn, batch_id, archive, user, workspace, target_dir):
        """Download a batch of files from the Object Store to a target
        directory.
        """
        # get the last part of the filepath
        object_name = os.path.basename(archive)
        download_file_path = os.path.join(target_dir, object_name)
        conn.download_file(batch_id, object_name, download_file_path)

    def create_batch(self, conn, user, workspace):
        """Create a batch on the object store and return the batch id.
        For the object store the batch id is the groupworkspace name appended
        with the next batch number for that groupworkspace.
        """
        gws_bucket_prefix = "gws-" + workspace + "-"
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

    def close_batch(self, conn, batch_id, user, workspace):
        """Close the batch on the external storage.
           Not needed for object store"""
        return

    def put(self, conn, batch_id, archive, user, workspace):
        """Put a staged archive (with path archive) onto the Object Store"""
        # get the last part of the filepath
        object_name = os.path.basename(archive)
        conn.upload_file(archive, batch_id, object_name)

    def user_has_put_permission(self, conn, username, workspace):
        """Check whether the user has permission (via their access_key and
        secret_key) to access the object store, and whether they have
        permission from the groupworkspace
        """
        # groupworkspace permission
        gws_permission = Backend.user_has_put_permission(
            self, conn, username, workspace
        )

        # to validate the credentials we have to do some operation, as just
        # connecting the client doesn't do any validation!
        try:
            conn.list_buckets()
            s3_permission = True
        except Exception:
            s3_permission = False
        return gws_permission & s3_permission

    def user_has_get_permission(self, conn, migration, username, workspace):
        """Check whether the user has permission (via their access_key and
        secret_key) to access the object store, and whether they have
        permission from the groupworkspace
        """
        gws_permission = Backend.user_has_get_permission(
            self, conn, migration, username, workspace
        )

        # to validate the credentials we have to do some operation, as just
        # connecting the client doesn't do any validation!
        try:
            conn.list_buckets()
            s3_permission = True
        except Exception:
            s3_permission = False
        return gws_permission & s3_permission

    def user_has_put_quota(self, conn, filelist, user, workspace):
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
