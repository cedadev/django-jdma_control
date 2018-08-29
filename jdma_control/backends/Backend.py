"""Generic class for a JASMIN Data Migration App backend.
All other backends should inherit from this class.
See FakeElasticTapeBackend for a fully documented example of a derived class.
"""

import logging
import os
import datetime

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_control.backends
import jdma_site.settings as settings


def get_backend_object(backend):
    found = False
    for be in jdma_control.backends.get_backends():
        bo = be()
        if bo.get_id() == backend:
            found = True
            JDMA_BACKEND_OBJECT = bo
    if found:
        return JDMA_BACKEND_OBJECT
    else:
        return None


class Backend(object):
    """Super class for all JASMIN Data Migration App backends.  All of these
    functions should be overloaded, i.e. the class is pure virtual.
    """

    def setup_logging(self, class_name):
        # setup the logging
        try:
            log_path = settings.LOG_PATH
        except Exception:
            log_path = "./"

        # Make the logging dir if it doesn't exist
        if not os.path.isdir(log_path):
            os.makedirs(log_path)

        date = datetime.datetime.utcnow()
        date_string = "%d%02i%02iT%02i%02i%02i" % (
            date.year,
            date.month,
            date.day,
            date.hour,
            date.minute,
            date.second
        )
        log_fname = log_path + "/" + self.__class__.__name__ + "_" + date_string

        logging.basicConfig(filename=log_fname, level=logging.DEBUG)

    def available(self, credentials):
        """Return whether the backend storage is avaliable at the moment
        - i.e. switched on or not!
        """
        return True

    def monitor(self):
        """Monitor the external storage, return which requests have completed"""
        raise NotImplementedError

    def pack_data(self):
        """Should the data be packed into a tarfile for this backend?"""
        raise NotImplementedError

    def piecewise(self):
        """Should the data be uploaded piecewise (archive by archive) or
        all at once?"""
        raise NotImplementedError

    def create_connection(self, user, workspace, credentials, mode="upload"):
        """Create a connection to the backend.
        We only want to do this once per bulk transfer as establishing the
        connection can take a significant portion of the runtime.
        """
        raise NotImplementedError

    def close_connection(self, conn):
        """Close the connection to the backend"""
        raise NotImplementedError

    def download_files(self, conn, get_req, file_list, target_dir):
        """Create a batch for download from a batch on the external storage,
        download the files and return the transfer id from the external storage"""
        raise NotImplementedError

    def upload_files(self, conn, put_req, prefix, file_list):
        """Create a batch for upload to the external storage, upload the files
        and set the batch id"""
        raise NotImplementedError

    def delete_batch(self, conn, del_req, batch_id):
        """Delete a batch from the external storage"""
        raise NotImplementedError

    # permissions / quota
    def user_has_put_permission(self, conn):
        """Does the user have permission to write to the workspace
        on the storage device?
        """
        raise NotImplementedError

    def _user_has_put_permission(self, username, workspace):
        """Does the user have permission to write to the workspace
        on the storage device?  LDAP version.
        """
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                                  settings.JDMA_LDAP_REPLICAS)

        # check workspace exists - get the group for the workspace from LDAP
        # LDAP workspaces have prefix of "gws_"
        with Connection.create(ldap_servers) as ldap_conn:
            query = Query(
                ldap_conn,
                base_dn=settings.JDMA_LDAP_BASE_GROUP
            ).filter(cn="gws_" + workspace)

            # check for a valid return
            if len(query) == 0:
                return False

            # check that user is in this workspace
            if username not in query[0]['memberUid']:
                return False
        return True

    def user_has_get_permission(self, batch_id, conn):
        """Does the user have permission to get the migration request from the
        storage device?"""
        raise NotImplementedError

    def _user_has_get_permission(self, username, workspace):
        """Does the user have permission to get a migration request from the
        storage device? This is a base example, can be overridden and also just
        called on its own.
        """
        # create the LDAP server pool needed in for the GET request
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                                  settings.JDMA_LDAP_REPLICAS)

        # all users in the Group Workspace have permission to read a file from
        # that workspace. Get the users in the workspace group
        with Connection.create(ldap_servers) as ldap_conn:
            query = Query(
                ldap_conn,
                base_dn=settings.JDMA_LDAP_BASE_GROUP
            ).filter(cn="gws_" + workspace)

            # check for a valid return
            if len(query) == 0:
                return False

            # check the user is in the workspace
            if username not in query[0]['memberUid']:
                return False

        return True

    def _user_has_delete_permission(self, username, workspace, batch_id):
        """Determine whether the user has the permission to delete the batch
        given by batch_id in the workspace.
        This should be determined by LDAP roles. i.e.:
        If the user owns the batch then they can delete it.
        If the user does not own the batch but is a manager of the workspace
        then they can delete it.
        """
        # avoid circular dependency
        from jdma_control.models import Migration, Groupworkspace

        # create the LDAP server pool needed in for the GET request
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                                  settings.JDMA_LDAP_REPLICAS)

        # check the user is a member of the group workspace
        with Connection.create(ldap_servers) as ldap_conn:
            query = Query(
                ldap_conn,
                base_dn=settings.JDMA_LDAP_BASE_GROUP
            ).filter(cn="gws_" + workspace)

            # check for a valid return
            if len(query) == 0:
                return False

            # check the user is in the workspace
            if username not in query[0]['memberUid']:
                return False

        # get the migration
        try:
            migration = Migration.objects.get(pk=batch_id)

            # does the user own the migration?
            if migration.user.name == username:
                return True
        except:
            return False

        # or, is the user a groupworkspace manager of the GWS
        try:
            group_workspace = Groupworkspace.objects.get(workspace=workspace)
            if len(group_workspace.managers.filter(name=username)) == 0:
                return False
        except:
            return False
        return True

    def user_has_delete_permission(self, batch_id, conn):
        """Determine whether the user has the permission to delete the batch
        given by batch_id in the workspace.
        """
        raise NotImplementedError

    def user_has_put_quota(self, conn):
        """Check the remaining quota for the user in the workspace"""
        raise NotImplementedError

    def get_name(self):
        return "Undefined"     # get the name for error messages

    def get_id(self):
        return "undefined"

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file
           in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the
           daemon processes can carry out the Migrations on behalf of the user.
        """
        return []

    def check_credentials_supplied(self, supplied_credentials):
        # check that the required credentials were supplied
        required_credentials = self.required_credentials()
        credentials_supplied = True
        for rk in required_credentials:
            if rk not in supplied_credentials:
                credentials_supplied = False
        return credentials_supplied

    def minimum_object_size(self):
        """The minimum recommended size for a file on this external storage
        medium.
        This should be overloaded in the inherited (super) classes
        """
        # in bytes - assume this is filesystem, so "optimum" is 32MB
        return 32 * 10**6
