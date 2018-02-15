"""Class for a JASMIN Data Migration App backend that targets an object store backend with S3 HTTP API.
   Uses boto3 API, but could easily be switched to use minio or another API.

   Creating a

   """

from jdma_control.backends.Backend import Backend
from jdma_control.backends import ObjectStoreSettings as OS_Settings

class ObjectStoreBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets an Object Store with S3 HTTP API.
       Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.VERIFY_DIR = OS_Settings.VERIFY_DIR


    def monitor(self):
        """Determine which batches have completed."""
        #completed_PUTs, completed_GETs = monitor_et_rss_feed(ET_Settings.ET_RSS_FILE)
        completed_PUTs = ["2"]
        completed_GETs = []
        return completed_PUTs, completed_GETs


    def get(self, batch_id, user, workspace, target_dir):
        """Download a batch of files from the Object Store to a target directory."""
        pass


    def put(self, filelist, user, workspace):
        """Put a list of files onto the Object Store - return the external storage batch id"""
        # create connection to Object Store
        batch_id = 0
        return int(batch_id)


    def user_has_put_permission(self, username, workspace):
        return Backend.user_has_put_permission(self, username, workspace)


    def user_has_get_permission(self, migration, username, workspace):
        return Backend.user_has_get_permission(self, migration, username, workspace)


    def user_has_put_quota(self, original_path, user, workspace):
        """Get the remaining quota for the user in the workspace.  How can we do this in Object Store backend?"""
        return True


    def get_name():
        return "Object Store"


    def get_id():
        return "objectstore"


    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the daemon processes can carry
           out the Migrations on behalf of the user."""
        return ["access_key", "secret_key"]
