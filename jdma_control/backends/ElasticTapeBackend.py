"""Class for a JASMIN Data Migration App backend which targets Elastic Tape."""

import os
from zlib import adler32
import tempfile
import socket # needed for ip address
import tarfile

from django.db.models import Q

import requests
from bs4 import BeautifulSoup

from jdma_control.backends.Backend import Backend
from jdma_control.backends import ElasticTapeSettings as ET_Settings
from jdma_control.backends.ConnectionPool import ConnectionPool
# Import elastic_tape client library
import elastic_tape.client as ET_client
import elastic_tape.shared as ET_shared
import elastic_tape.shared.storaged_pb2 as ET_proto

import jdma_site.settings as settings
from time import sleep

# create the connection pool - these are needed for the get transfers, as each
# transfer thread requires a connection that is kept up
et_connection_pool = ConnectionPool()

def get_completed_puts():
    """Get all the completed puts for the Elastic Tape"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")
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
        # get a list of synced files for this workspace and user and batch
        holdings_url = "{}?batch={};workspace={};caller={};level=file".format(
            ET_Settings.ET_HOLDINGS_URL,
            pr.migration.external_id,
            pr.migration.workspace.workspace,
            pr.migration.user.name
        )
        # use requests to fetch the URL
        r = requests.get(holdings_url)
        if r.status_code == 200:
            bs = BeautifulSoup(r.content, "xml")
        else:
            raise Exception(ET_Settings.ET_ROLE_URL + " is unreachable.")

        # get the "file" tags
        files = bs.select("file")
        # count the number of synced files
        n_synced = 0
        # count the number of archives
        n_archives = pr.migration.migrationarchive_set.count()
        # loop over these files
        for f in files:
            state = f.find("current_state").text.strip()
            file_path = f.find("file_name").text.strip()
            # state is SYNCED for completed PUT transfers
            if state == "SYNCED" or state == "CACHED_SYNCED":
                n_synced += 1
        # if the number of synced is equal to the number of archives then the
        # PUT has completed
        if n_synced == n_archives:
            completed_PUTs.append(pr.migration.external_id)
    return completed_PUTs


def get_completed_gets():
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, StorageQuota, MigrationArchive
    global et_connection_pool
    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")

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
        # create or find a connection to the ET server
        new_conn = et_connection_pool.find_or_create_connection(
            self,
            gr,
            None,
            mode="download",
            thread_number=None
        )        # use the elastic tape library to query the retrieval request and
        # add to completed_GETs if the retrieval has completed
        try:
            if new_conn.msgIface.checkRRComplete(int(gr.transfer_id)):
                completed_GETs.append(gr.transfer_id)
                # close the transfer
                badFiles = new_conn.msgIface.FinishRR(int(gr.transfer_id))
        except:
            # Old transfer ids hanging around?
            pass

    return completed_GETs


def get_completed_deletes():
    """Get all the completed deletes for the ObjectStore"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")

    # list of completed DELETEs to return
    completed_DELETEs = []
    # now loop over the PUT requests
    del_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.DELETE))
        & Q(stage=MigrationRequest.DELETING)
        & Q(migration__storage__storage=storage_id)
    )
    for dr in del_reqs:
        pass
    return completed_DELETEs


def user_in_workspace(jdma_user, jdma_workspace):
    """Determine whether a user is in a workspace by using requests to fetch
    a URL and beautifulsoup to parse the table returned.
    We'll ask Kevin O'Neill to provide a JSON version of this."""

    # get from requests
    r = requests.get(ET_Settings.ET_ROLE_URL)
    if r.status_code == 200:
        bs = BeautifulSoup(r.content, "html5lib")
    else:
        raise Exception(ET_Settings.ET_ROLE_URL + " is unreachable.")

    # parse into dictionary from table
    gws_roles = {}
    current_gws = ""
    for row in bs.select("tr"):
        if row is not None:
            cells = row.findAll("td")
            if len(cells) >= 4:
                # get the group workspace
                gws = cells[0].text.strip()
                user = cells[2].text.strip()
                if len(gws) > 0:
                    current_gws = gws
                    gws_roles[current_gws] = [user]
                else:
                    gws_roles[current_gws].append(user)

    # no roles were returned
    if gws_roles == {}:
        raise Exception(ET_Settings.ET_ROLE_URL + " did not return a valid list"
                        " of roles")
    # check if workspace exists
    if jdma_workspace not in gws_roles:
        return False
    else:
        return jdma_user in gws_roles[jdma_workspace]


def workspace_quota_remaining(jdma_user, jdma_workspace):
    """Get the workspace quota by using requests to fetch a URL.  Unfortunately,
    the JSON version of this URL returns ill-formatted JSON with a XML header!
    So we can't just parse that, and we use the regular HTML table view and
    parse using beautifulsoup again."""
    # form the URL
    url = "{}{}{}{}{}".format(ET_Settings.ET_QUOTA_URL,
                              "?workspace=", jdma_workspace,
                              ";caller=", jdma_user)
    # fetch using requests
    r = requests.get(url)
    if r.status_code == 200:
        # success, so parse the json
        bs = BeautifulSoup(r.content, "html5lib")
    else:
        raise Exception(url + " is unreachable.")

    quota_allocated = -1
    quota_used = -1
    for row in bs.select("tr"):
        if row is not None:
            cells = row.findAll("td")
            # quota_allocated is position 4, quota_used is position 5 (both in bytes)
            if len(cells) == 7:
                quota_allocated = int(cells[4].text.strip())
                quota_used = int(cells[5].text.strip())

    # check that valid quotas were returned
    if quota_allocated == -1 or quota_used == -1:
        raise Exception(url + " did not return a quota.")

    return quota_allocated - quota_used


class ElasticTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets Elastic Tape.
    Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and archive staging directory"""
        self.VERIFY_DIR = ET_Settings.VERIFY_DIR
        self.ARCHIVE_STAGING_DIR = ET_Settings.ARCHIVE_STAGING_DIR

    def available(self, credentials):
        """Return whether the elastic tape is available or not"""
        try:
            return True
        except Exception:
            return False

    def monitor(self):
        """Determine which batches have completed."""
        try:
            completed_PUTs = get_completed_puts()
            completed_GETs = get_completed_gets()
            completed_DELETEs = get_completed_deletes()
        except Exception as e:
            raise Exception(e)
        return completed_PUTs, completed_GETs, completed_DELETEs

    def create_connection(self, user, workspace, credentials, mode="upload"):
        """Create connection to Elastic Tape, using the supplied credentials.
        (There are no required credentials!)
        """
        if mode == "upload" or mode == "delete":
            conn = ET_client.client.client(ET_Settings.PUT_HOST, ET_Settings.PORT)
        elif mode == "download":
            conn = ET_client.client.client(ET_Settings.GET_HOST, ET_Settings.PORT)

        conn.connect()
        # save the user and workspace
        conn.jdma_user = user
        conn.jdma_workspace = workspace
        return conn

    def close_connection(self, conn):
        """Close the connection to the backend.
        """
        conn.close()
        return

    def create_download_batch(self, conn, external_id, file_list=[], target_dir=""):
        """Create a download batch for the elastic tape.
        This will also instigate the transfer, and run the transfers, as ET
        requires the conn to stay up during the transfer.
        """
        # the ET client interface is contained in ET_conn
        try:
            # don't do anything if filelist length is 0
            if len(file_list) == 0:
                return

            # Get the ip address of the sender
            ip = socket.gethostbyname(socket.gethostname())

            # create a new batch
            batch = conn.newBatch(conn.jdma_workspace, None)
            # override the requester in the Batch
            batch.requestor = conn.jdma_user
            # override the ip address
            batch.PI = ip
            # override the override
            batch.override = 0

            # add the files to the batch
            for f in file_list:
                batch.addFile(f)

            # register a batch retrieval
            batch_retrieve = batch.retrieve()
            transfer_id = conn.msgIface.retrieveBatch(batch_retrieve)
            # start the retrieval
            conn.msgIface.sendStartRetrieve(int(transfer_id))

        except Exception as e:
            transfer_id = None
            raise Exception(str(e))
        return str(transfer_id)

    def close_download_batch(self, conn, transfer_id):
        """Close the download batch for the elastic tape."""
        return

    def get(self, conn, transfer_id, archive, target_dir, thread_number=None):
        """Download a number files from the elastic tape to a target directory.
        We can run this function in a thread to parallelise the transfers.
        """
        # avoid a circular dependancy
        from jdma_control.models import MigrationRequest, StorageQuota
        # get the storage id for the backend object
        storage_id = StorageQuota.get_storage_index(self.get_id())
        gr = MigrationRequest.objects.get(
            Q(request_type=MigrationRequest.GET)
            & Q(transfer_id=transfer_id)
            & Q(migration__storage__storage=storage_id)
        )
        # The ET library requires a separate connection for each download, and
        # a marshalling connection (which is conn)
        global et_connection_pool
        new_conn = et_connection_pool.find_or_create_connection(
            self,
            gr,
            None,       # are credentials needed?
            mode="download",
            thread_number=thread_number
        )

        # get the next processable / transferrable tar file
        new_conn.msgIface.sendNextProcessable(int(transfer_id))
        # read the files to transfer
        trans_data = new_conn.msgIface.readFiles()
        # print(int(transfer_id), trans_data)
        # if trans_data is None or data is finished then exit. As the transfer
        # runs as a daemon, an attempt to download will be made next time the
        # loop occurs
        if trans_data is None:
            # leave the connection open
            return 0

        if trans_data.finished:
            # finished - delete the connection
            et_connection_pool.close_connection(
                self,
                transfer_id,
                thread_number=thread_number
            )

        if trans_data.errored:
            et_connection_pool.close_connection()
            raise Exception("Error in ElasticTapeBackend::get")

        # check that type is "CLIENT_TAR" - it should be as all transfers are
        # tar files
        if not trans_data.type == "CLIENT_TAR":
            raise Exception(
                "ElasticTapeBackend::get trying to download a non tar file."
            )
        # it is a tar file so download it as such and unpack it
        # this code comes from the __getTar function in elastic_tape.client

        checksum = 1
        # Download tar to a temporary file to the target directory
        fd, tempname = tempfile.mkstemp(dir=target_dir)
        handle = os.fdopen(fd, 'wb')

        while True: # loop while transferring the data, exit via the break below
            try:
                msg = new_conn.msgIface.readMessage()
                if msg.Type == ET_proto.DATA:
                    checksum = adler32(msg.Payload, checksum)
                    handle.write(msg.Payload)
                elif msg.Type == ET_proto.CHECKSUM:
                    resp = ET_proto.File()
                    try:
                        resp.ParseFromString(msg.Payload)
                    except google.protobuf.message.DecodeError as e:
                        raise ET_shared.error.StorageDError (
                            ET_shared.error.EBADRESP,
                            notes="Expected Checksum"
                        )

                    if ET_shared.checksum.genChecksum(checksum) != resp.Checksum:
                        raise ET_shared.error.StorageDError (
                            ET_shared.error.ERETRIEVE,
                            notes="Bad checksum for tar transfer"
                        )
                    else: # Indicates the end of a successful download of tar data
                        break
                else:
                    raise ET_shared.error.StorageDError (
                        ET_shared.error.EBADRESP,
                        notes="Unexpected message type {}".format(msg.Type)
                    )
            except ET_shared.error.StorageDError as e:
                raise ET_shared.error.StorageDError(
                    'Received error %s on downloading tar data'
                )

        # Once data has been downloaded and checksum verified, unpack the tar file
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()

        # extract the tar file
        try:
            tarData = tarfile.open(tempname)
            for f in fileset:
                tarname = f.fileDetails.Filename
                tarData.extract(tarname.lstrip('/'), target_dir)
            # delete the tarfile
            os.unlink(tempname)
        except:
            raise Exception(
                "Error extracting ET tar file: {}".format(tempname)
            )

        return 1

    def create_upload_batch(self, conn, batch_name="", file_list=[]):
        """Create a batch on the elastic tape and upload the filenames.
        The batch id will be created.
        """
        try:
            # don't do anything if filelist length is 0
            if len(file_list) == 0:
                return

            # Get the ip address of the sender
            ip = socket.gethostbyname(socket.gethostname())

            # create a new batch
            batch = conn.newBatch(conn.jdma_workspace, batch_name)
            # override the requester in the Batch
            batch.requestor = conn.jdma_user
            # override the ip address
            batch.PI = ip
            # override the override
            batch.override = 0

            # add the files to the batch
            for f in file_list:
                batch.addFile(f)

            # close the batch and get the batch id - convert to string on return
            # from function
            batch_id = batch.register()

        except Exception as e:
            batch_id = None
            raise Exception(str(e))
        return str(batch_id)

    def close_upload_batch(self, conn, batch_id):
        """Close the batch on the elastic tape. The ET already has an
        asyncronous structure so replicating it here is not necessary."""
        return

    def put(self, conn, batch_id, archive):
        """Put a staged archive (with path archive) onto the elastic tape.
        Here we add to the conn.files list of files, which the names of are
        all uploaded on close_upload_batch
        """

        try:
            # get the next transferrable for this batch and ip address
            ip = socket.gethostbyname(socket.gethostname())
            transfer = conn.getNextTransferrable(PI=ip, batchID=int(batch_id))
            # Handle the transfer
            if transfer != None:
                transfer.send()
        except ET_shared.error.StorageDError as e:
            if e.code == ET_shared.error.ECCHEFUL:
                # cache is full, do nothing, jdma_transfer will try again later
                pass
            else:
                # some other error - raise an Exception and the migration will
                # be set to FAILED
                raise Exception(e)
        except Exception as e:
            raise Exception(e)
        # return zero for the last_archive count - ET does not need this to keep
        # a count of which archives have uploaded
        return 0

    def create_delete_batch(self, conn):
        """Create a batch to delete files from the elastic tape"""
        return None

    def close_delete_batch(self, conn, batch_id):
        """Close the delete batch on the elastic tape"""
        return None

    def delete(self, conn, batch_id, archive):
        """Delete a single tarred archive of files from the object store"""
        object_name = os.path.basename(archive)
        conn.delete_object(Bucket=batch_id, Key=object_name)

    def user_has_put_permission(self, conn):
        """Check whether the user has permission to access the elastic tape,
        and whether they have permission from the groupworkspace
        """
        # groupworkspace permission
        gws_permission = Backend._user_has_put_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )

        # elastic tape permission - fetch from URL and use beautifulsoup to
        # parse the returned table into something meaningful
        et_permission = user_in_workspace(
            conn.jdma_user, conn.jdma_workspace.workspace
        )

        return gws_permission & et_permission

    def user_has_get_permission(self, batch_id, conn):
        """Check whether the user has permission to access the elastic tape,
        and whether they have permission from the groupworkspace
        """
        gws_permission = Backend._user_has_get_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )

        # elastic tape permission
        et_permission = user_in_workspace(
            conn.jdma_user, conn.jdma_workspace.workspace
        )

        return gws_permission & et_permission

    def user_has_delete_permission(self, batch_id, conn):
        """Check whether the user has permission to delete the object from the
        elastic tape, and whether they have permission from the groupworkspace
        LDAP.
        """
        # check from the groupworkspace
        gws_permission = Backend._user_has_delete_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace, batch_id
        )

        # elastic tape permission
        et_permission = user_in_workspace(
            conn.jdma_user, conn.jdma_workspace.workspace
        )

        return gws_permission & et_permission

    def user_has_put_quota(self, conn):
        """Check the remaining quota for the user in the workspace.
        We just check the database here, i.e. check that we are not over
        quota.
        When jdma_lock calculates the file sizes we can check the quota again
        and flag the transfer as FAILED if it goes over the quota.
        """
        from jdma_control.models import StorageQuota
        # get the storage id
        storage_id = StorageQuota.get_storage_index("elastictape")
        storage_quota = StorageQuota.objects.filter(
            storage=storage_id,
            workspace__workspace=conn.jdma_workspace
        )[0]
        jdma_quota_remaining = storage_quota.quota_size - storage_quota.quota_used

        # get the quota from the elastic tape feed
        et_quota_remaining = workspace_quota_remaining(
            conn.jdma_user, conn.jdma_workspace.workspace
        )

        return (jdma_quota_remaining > 0) & (et_quota_remaining > 0)

    def get_name(self):
        return "Elastic Tape"

    def get_id(self):
        return "elastictape"

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
        These keys, along with their values, will be stored in a hidden file
        in the user's home directory.
        They will be encrypted and stored in the MigrationRequest so that
        the daemon processes can carry out the Migrations on behalf of the
        user.
        """
        return []

    def minimum_object_size(self):
        """Minimum recommend size for elastic tape = 2GB? (check with Kevin
        O'Neil)
        """
        return ET_Settings.OBJECT_SIZE
