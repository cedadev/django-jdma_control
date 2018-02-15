"""Generic class for a JASMIN Data Migration App backend.
   All other backends should inherit from this class.
   See FakeTapeBackend for a fully documented example of a derived class."""

from jasmin_ldap.core import *
from jasmin_ldap.query import *

from jdma_control.models import *
import jdma_control.backends
import jdma_site.settings as settings
import logging
import os
import datetime


def get_backend_object(backend):
    found = False
    for be in jdma_control.backends.get_backends():
        if be.get_id() == backend:
            found = True
            JDMA_BACKEND_OBJECT = be()
    if found:
        return JDMA_BACKEND_OBJECT
    else:
        return None


def encrypt_supplied_credentials(supplied_credentials):
    """Credentials are supplied as key:value pairs.  They are then stored in the JDMA database as
       part of the MigrationRequest model.  Before storage they are encrypted by this function,
       using the encryption details in {{ application_home }}/conf/encrypt_key.txt which is
       installed during the ansible deployment"""
    # get the encryption key


class Backend(object):
    """Super class for all JASMIN Data Migration App backends.abs
       All of these functions should be overloaded, i.e. the class is pure virtual."""

    def setup_logging(self, class_name):
        # setup the logging
        try:
            log_path = settings.LOG_PATH
        except:
            log_path = "./"

        # Make the logging dir if it doesn't exist
        if not os.path.isdir(log_path):
            os.makedirs(log_path)

        date = datetime.datetime.utcnow()
        date_string = "%d%02i%02iT%02i%02i%02i" % (date.year, date.month, date.day, date.hour, date.minute, date.second)
        log_fname = log_path + "/" + self.__class__.__name__ + "_" + date_string

        logging.basicConfig(filename=log_fname, level=logging.DEBUG)


    def monitor(self):
        """Monitor the external storage, return which requests have completed"""
        raise NotImplementedError

    def get(self, batch_id, user, workspace, target_dir):
        """Get the batch from the external storage and download to a target_dir"""
        raise NotImplementedError

    def put(self, filelist, user, workspace):
        """Put a list of files onto the external storage - return the external storage batch id"""
        raise NotImplementedError

    # permissions / quota
    def user_has_put_permission(self, username, workspace):
        """Does the user have permission to write to the workspace on the storage device"""
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

        # check workspace exists - get the group for the workspace from LDAP
        # LDAP workspaces have prefix of "gws_"
        with Connection.create(ldap_servers) as conn:
            query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn="gws_"+workspace)

            # check for a valid return
            if len(query) == 0:
                return False

            # check that user is in this workspace
            if username not in query[0]['memberUid']:
                return False
        return True


    def user_has_get_permission(self, migration, username, workspace):
        """Does the user have permission to get the migration request from the storage device.
           This is a base example, can be overridden and also just called on its own."""
        # create the LDAP server pool needed in both GET and PUT requests
        ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

        # all users in the Group Workspace have permission to read a file from that workspace
        # get the users in the workspace group
        with Connection.create(ldap_servers) as conn:
            query = Query(conn, base_dn=settings.JDMA_LDAP_BASE_GROUP).filter(cn="gws_"+workspace)

            # check for a valid return
            if len(query) == 0:
                return False

            # check the user is in the workspace
            if username not in query[0]['memberUid']:
                return False

        return True


    def user_has_put_quota(self, filelist, username, workspace):
        """Get the remaining quota for the user in the workspace"""
        return False


    def get_name():
        return "Undefined"     # get the name for error messages


    def get_id():
        return "undefined"


    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the daemon processes can carry
           out the Migrations on behalf of the user."""
        return []


    def check_credentials_supplied(self, supplied_credentials):
        # check that the required credentials were supplied
        required_credentials = self.required_credentials()
        credentials_supplied = True
        for rk in required_credentials:
            if not rk in supplied_credentials:
                credentials_supplied = False
        return credentials_supplied
