"""Class for a JASMIN Data Migration App backend which targets Elastic Tape.
Note that this is the simplified version, which (simply) uses functions from
the Elastic Tape client to GET and PUT the data.
Transport is handled by the script ET_transfer_mp, which is a version of the
et_transfer_mp script which runs on et1.ceda.ac.uk."""

import os

from django.db.models import Q

import requests
from bs4 import BeautifulSoup
from time import sleep
import subprocess
import logging
from datetime import datetime
import dateutil.parser

from jdma_control.backends.Backend import Backend
from jdma_control.backends.ConnectionPool import ConnectionPool
from jdma_control.scripts.common import get_ip_address
from jdma_control.scripts.config import read_backend_config

# Import elastic_tape client library
import elastic_tape.client as ET_client

import jdma_site.settings as settings

# create the connection pool - these are needed for the get transfers, as each
# transfer thread requires a connection that is kept up
et_connection_pool = ConnectionPool()

class ETException(Exception):
    pass

def get_completed_puts(backend_object):
    """Get all the completed puts for the Elastic Tape"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")
    # list of completed PUTs to return
    completed_PUTs = []
    ET_Settings = backend_object.ET_Settings

    # now loop over the PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.PUTTING)
        & Q(migration__stage=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        if pr.migration.external_id is None:
            continue
        # form the url and get the response, parse the document using bs4
        holdings_url = "{}?batch={}".format(
	    ET_Settings["ET_INPUT_BATCH_SUMMARY_URL"],
            pr.migration.external_id
        )
        sleep(0.1)  # 100 ms delay to avoid overloading the server

        r = requests.get(holdings_url)
        if r.status_code == 200:
            bs = BeautifulSoup(r.content, "xml")

        # get all the tables and then check the 2nd
        # get the 2nd table - 1st is just a heading table
        tables = bs.find_all("table")
        if len(tables[1]) == 0:
            continue

        # get the first row of the 2nd table
        rows = tables[1].find_all("tr")
        if len(rows) < 2:
            continue

        # the status is the first column
        cols = rows[1].find_all("td")
        if len(cols) < 3:
            continue

        status = cols[0].get_text()
        # check for completion
        if status in ["SYNCED", "TAPED"]:
            # check for a pause - read the 3rd (2) table as that has
            # a "Time to Tape" date in the 4th column of the 2nd row
            # we need to check every row to determine which is the latest time
            if len(tables[2]) == 0:
                continue
            
            rows = tables[2].find_all("tr")
            if len(rows) < 2:
                continue
            last_time_to_tape = datetime(year=1, month=1, day=1)
            # loop over each row
            for r in rows[1:]:
                cols = r.find_all("td")
                if len(cols) < 4:
                    continue
                # get the time / date the file was loaded and convert to datetime
                time_to_tape = dateutil.parser.isoparse(cols[3].get_text())

                if time_to_tape > last_time_to_tape:
                    last_time_to_tape = time_to_tape

            # now check that time against now
            delta = datetime.now() - last_time_to_tape
            if (delta.days > 0) or (delta.seconds > settings.JDMA_VERIFY_PAUSE):
                #print("Completed PUT", pr.migration.external_id, "at", last_time_to_tape)
                completed_PUTs.append(pr.migration.external_id)

    return completed_PUTs


def get_completed_gets(backend_object):
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, StorageQuota, MigrationArchive
    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")
    ET_Settings = backend_object.ET_Settings

    # list of completed GETs to return
    completed_GETs = []
    # now loop over the GET requests
    get_reqs = MigrationRequest.objects.filter(
        (Q(stage=MigrationRequest.GETTING)
        | Q(stage=MigrationRequest.VERIFY_GETTING))
        & Q(migration__storage__storage=storage_id)
    )
    #
    backend = ElasticTapeBackend()
    for gr in get_reqs:
        if gr.transfer_id is None:
            continue
        # get a list of synced files for this workspace and user and batch
        retrieval_url = "{}?rr_id={};workspace={}".format(
            ET_Settings["ET_RETRIEVAL_URL"],
            gr.transfer_id,
            gr.migration.workspace.workspace,
        )
        # use requests to fetch the URL
        sleep(0.1)  # 100 ms delay to avoid overloading the server
        r = requests.get(retrieval_url)
        if r.status_code == 200:
            bs = BeautifulSoup(r.content, "xml")
        else:
            logging.error("Error in ET monitor:{} is unreachable".format(str(holdings_url)))
            continue

        # get the 2nd table from beautiful soup
        table = bs.find_all("table")[1]
        # check that a table has been found - there might be a slight
        # synchronisation difference between jdma_transfer and jdma_monitor
        # i.e. the entry might be in the database but not updated on the
        # RETRIEVAL_URL
        if len(table) == 0:
            continue
        # get the first row
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        row_1 = table.find_all("tr")[1]

        # the transfer id is the first column, the status is the third
        cols = row_1.find_all("td")
        if len(cols) < 3:
            continue
        transfer_id = cols[0].get_text()
        status = cols[2].get_text()
        # this is a paranoid check - this really shouldn't happen!
        if (transfer_id != gr.transfer_id):
            raise ETException("Transfer id mismatch")
        # check for completion
        if status == "COMPLETED":
            completed_GETs.append(gr.transfer_id)

    return completed_GETs


def get_completed_deletes(backend_object):
    """Get all the completed deletes for the Elastic Tape"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota    # get the storage id
    storage_id = StorageQuota.get_storage_index("elastictape")
    ET_Settings = backend_object.ET_Settings

    # list of completed DELETEs to return
    completed_DELETEs = []
    # now loop over the PUT requests
    del_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.DELETE))
        & Q(stage=MigrationRequest.DELETING)
        & Q(migration__storage__storage=storage_id)
    )
    for dr in del_reqs:
        # assume deleted
        deleted = True
        # get a list of synced batches for this workspace and user
        holdings_url = "{}?workspace={};caller={};level=batch".format(
            ET_Settings["ET_HOLDINGS_URL"],
            dr.migration.workspace.workspace,
            dr.migration.user.name
        )
        # use requests to fetch the URL
        sleep(0.1)  # 100 ms delay to avoid overloading the server
        r = requests.get(holdings_url)
        if r.status_code == 200:
            bs = BeautifulSoup(r.content, "xml")
        else:
            logging.error("Error in ET monitor:{} is unreachable".format(str(holdings_url)))
            continue

        # if the dr.migration.external_id is not in the list of batches
        # then the delete has completed
        batches = bs.select("batch")
        for b in batches:
            batch_id = b.find("batch_id").text.strip()
            if batch_id == dr.migration.external_id:
                deleted = False

        if deleted:
            # it's been deleted so add to the returned list of completed DELETEs
            completed_DELETEs.append(dr.migration.external_id)
    return completed_DELETEs


def user_in_workspace(jdma_user, jdma_workspace, ET_Settings):
    """Determine whether a user is in a workspace by using requests to fetch
    a URL and beautifulsoup to parse the table returned.
    We'll ask Kevin O'Neill to provide a JSON version of this."""

    # get from requests
    sleep(0.1)  # 100 ms delay to avoid overloading the server
    r = requests.get(ET_Settings["ET_ROLE_URL"])
    if r.status_code == 200:
        bs = BeautifulSoup(r.content, "html5lib")
    else:
        raise ETException(ET_Settings["ET_ROLE_URL"] + " is unreachable.")

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
        raise ETException(
            ET_Settings["ET_ROLE_URL"] + " did not return a valid list of roles"
        )
    # check if workspace exists
    if jdma_workspace not in gws_roles:
        return False
    else:
        return jdma_user in gws_roles[jdma_workspace]


def workspace_quota_remaining(jdma_user, jdma_workspace, ET_Settings):
    """Get the workspace quota by using requests to fetch a URL.  Unfortunately,
    the JSON version of this URL returns ill-formatted JSON with a XML header!
    So we can't just parse that, and we use the regular HTML table view and
    parse using beautifulsoup again."""
    # form the URL
    url = "{}{}{}{}{}".format(ET_Settings["ET_QUOTA_URL"],
                              "?workspace=", jdma_workspace,
                              ";caller=", jdma_user)
    # fetch using requests
    sleep(0.1)  # 100 ms delay to avoid overloading the server
    r = requests.get(url)
    if r.status_code == 200:
        # success, so parse the json
        bs = BeautifulSoup(r.content, "html5lib")
    else:
        raise ETException(url + " is unreachable.")

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
        raise ETException(url + " did not return a quota.")

    return quota_allocated - quota_used

class ElasticTapeBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets Elastic Tape.
    Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and archive staging directory"""
        self.ET_Settings = read_backend_config(self.get_id())
        self.VERIFY_DIR = self.ET_Settings["VERIFY_DIR"]
        self.ARCHIVE_STAGING_DIR = self.ET_Settings["ARCHIVE_STAGING_DIR"]

    def exit(self):
        """Shutdown the backend. Do nothing for ET."""
        return


    def available(self, credentials):
        """Return whether the elastic tape is available or not"""
        try:
            return "available"
        except Exception:
            return "not available"

    def monitor(self, thread_number=None):
        """Determine which batches have completed."""
        completed_PUTs = []
        completed_GETs = []
        completed_DELETEs = []

        try:
            completed_PUTs = get_completed_puts(self)
        except SystemExit:
            completed_PUTs = []
        except ETException as e:
            logging.error("Error in ET monitor: {}".format(str(e)))


        try:
            completed_GETs = get_completed_gets(self)
        except SystemExit:
            completed_GETs = []
        except ETException as e:
            logging.error("Error in ET monitor: {}".format(str(e)))

        try:
            completed_DELETEs = get_completed_deletes(self)
        except SystemExit:
            completed_DELETEs = []
        except ETException as e:
            logging.error("Error in ET monitor: {}".format(str(e)))

        return completed_PUTs, completed_GETs, completed_DELETEs

    def pack_data(self):
        """Should the data be packed into a tarfile for this backend?"""
        return False

    def piecewise(self):
        """For elastic tape the data shouldn't be uploaded archive by archive
        but uploaded all at once."""
        return False

    def create_connection(self, user, workspace, credentials, mode="upload"):
        """Create connection to Elastic Tape, using the supplied credentials.
        (There are no required credentials!)
        """
        if mode == "upload" or mode == "delete":
            conn = ET_client.client.client(
                self.ET_Settings["PUT_HOST"],
                self.ET_Settings["PORT"]
            )
        elif mode == "download":
            conn = ET_client.client.client(
                self.ET_Settings["GET_HOST"],
                self.ET_Settings["PORT"]
            )

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

    def download_files(self, conn, get_req, file_list, target_dir):
        """Create a download batch for the elastic tape.
        This will also instigate the transfer, as ET requires the conn to stay
        up during the transfer.
        """
        # the ET client interface is contained in ET_conn
        try:
            # don't do anything if filelist length is 0
            if len(file_list) == 0:
                return

            # get the external id
            external_id = get_req.migration.external_id
            if external_id is None:
                return
            # Get the ip address of the sender
            ip = get_ip_address()

            # create a new batch
            batch = conn.newBatch(conn.jdma_workspace, None)
            # override the requester in the Batch
            batch.requestor = conn.jdma_user
            # override the ip address
            batch.PI = ip
            # overwrite any files
            batch.override = 1

            # get the common_path
            cp = get_req.migration.common_path
            # add the files to the batch
            for f in file_list:
                fname = os.path.join(cp,f)
                batch.addFile(fname)

            """The code below here is replicated from elastic_tape.client.client
            We need to replicate it as we need to get the transfer id for
            monitoring purposes."""

            # get the files from the ET client
            retrieve_batch = batch.retrieve()

            # get the request id and store it in the migration request
            reqID = conn.msgIface.retrieveBatch(retrieve_batch)
            get_req.transfer_id = reqID
            get_req.save()

            conn.msgIface.sendStartRetrieve(reqID)

            downloadThreads = []
            handler = ET_client.client.DownloadThread

            for i in range(self.ET_Settings["THREADS"]):
                t = handler()
                t.daemon = True
                downloadThreads.append(t)
                t.setup(reqID, target_dir, conn.host, conn.port)
                t.start()

            while not conn.msgIface.checkRRComplete(reqID):
                sleep(5)

            bad_files = conn.msgIface.FinishRR(reqID)
            for t in downloadThreads:
                t.stop()
                t.join()

            # raise the list of badfiles as an exception - mark migraiton as
            # FAILED
            if len(bad_files) > 0:
                raise Exception(
                    "Could not download files: {}".format(bad_files)
                )
            # elastic tape copies these files to a directory that looks like:
            # /target_dir/group_workspaces/jasmin4/gws_name/user_name/original_directory
            # whereas what we want them to look like :
            # /target_dir/original_directory
            # This can be acheived by using the original path of the migration
            # and moving the files from the /target_dir/common_path... to just
            # the target dir
            # we have to trim the first character from the common path (which is
            # a / to get os.path.join to join the paths correctly)
            source_dir_cp = os.path.join(target_dir, cp[1:])
            # get a list of all the files in the source directory
            for f in os.listdir(source_dir_cp):
                full_source_path = "{}/{}".format(source_dir_cp, f)
                subprocess.call(["/bin/mv", full_source_path, target_dir])
            # we now want to delete the empty directories that are left after the move
            # this is everything beneath /target_dir/first_directory_of_common_path
            dir_to_remove = os.path.join(target_dir, cp.split("/")[1])
            subprocess.call(["/bin/rm", "-r", dir_to_remove])

        except Exception as e:
            raise Exception(str(e))
        return str(external_id)

    def upload_files(self, conn, put_req, prefix, file_list):
        """Create a batch on the elastic tape and upload the filenames.
        The batch id will be created and saved to the Migration.
        """
        try:
            # don't do anything if filelist length is 0
            if len(file_list) == 0:
                return

            # Get the ip address of the sender
            ip = get_ip_address()

            batch_name = put_req.migration.label
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

            # register the batch_id as the external id
            put_req.migration.external_id = batch_id
            put_req.migration.save()

        except Exception as e:
            batch_id = None
            raise ETException(str(e))
        return str(batch_id)


    def delete_batch(self, conn, del_req, batch_id):
        """Delete a archive of files from the elastic tape"""
        conn.deleteBatchByID(conn.jdma_workspace,
                             conn.jdma_user,
                             int(batch_id))

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
            conn.jdma_user,
            conn.jdma_workspace.workspace,
            self.ET_Settings
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
            conn.jdma_user,
            conn.jdma_workspace.workspace,
            self.ET_Settings
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
            conn.jdma_user,
            conn.jdma_workspace.workspace,
            self.ET_Settings
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
            conn.jdma_user,
            conn.jdma_workspace.workspace,
            self.ET_Settings,
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
        return int(self.ET_Settings["OBJECT_SIZE"])

    def maximum_object_count(self):
        """Maximum number of objects in an archive"""
        return (int(self.ET_Settings["OBJECT_COUNT"]))
