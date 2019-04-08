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
from jdma_control.scripts.config import read_backend_config
from jdma_control.backends.ConnectionPool import ConnectionPool
from jdma_control.backends import AES_tools
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import get_verify_dir, get_staging_dir, get_download_dir
import jdma_site.settings as settings

import multiprocessing
import signal

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
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.PUTTING)
        & Q(migration__stage=Migration.PUTTING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # decrypt the credentials
        credentials = AES_tools.AES_decrypt_dict(key, pr.credentials)
        try:
            # create a connection to the object store
            s3c = boto3.client(
                "s3",
                endpoint_url=backend_object.OS_Settings["S3_ENDPOINT"],
                aws_access_key_id=credentials['access_key'],
                aws_secret_access_key=credentials['secret_key']
            )
            # loop over each archive in the migration
            archive_set = pr.migration.migrationarchive_set.order_by('pk')
            # counter for number of uploaded archives
            n_up_arch = 0
            for archive in archive_set:
                # get the list of files for this archive
                if archive.packed:
                    file_list = [archive.get_archive_name()]
                else:
                    file_list = archive.get_file_names()['FILE']
                n_files = 0
                for file_path in file_list:
                    # object name is the file_path, without any prefix
                    try:
                        if s3c.head_object(Bucket=pr.migration.external_id,
                                           Key=file_path):
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
                    staging_dir = get_download_dir(backend_object, gr)
                else:
                    staging_dir = gr.target_path
            # get filelist or single archive name
            if archive.packed:
                file_name_list = [archive.get_archive_name()]
            else:
                file_name_list = archive.get_file_names(
                    filter_list=gr.filelist,
                )['FILE']

            # now loop over each file in the archive
            n_completed_files = 0
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
            s3c = boto3.client(
                "s3",
                endpoint_url=backend_object.OS_Settings["S3_ENDPOINT"],
                aws_access_key_id=credentials['access_key'],
                aws_secret_access_key=credentials['secret_key']
            )
            # if the bucket has been deleted then the deletion has completed
            buckets = s3c.list_buckets()
            if ('Buckets' not in buckets
                 or dr.migration.external_id not in buckets['Buckets']):
                completed_DELETEs.append(dr.migration.external_id)
        except Exception as e:
            raise Exception(e)
    return completed_DELETEs


class OS_DownloadProcess(multiprocessing.Process):
    """Download thread for Object Store backend."""
    def setup(self,
              filelist,
              external_id,
              req_number,
              target_dir,
              credentials,
              backend_object,
              thread_number
        ):
        self.filelist = filelist
        self.conn = backend_object.connection_pool.find_or_create_connection(
            backend_object,
            req_number = req_number,
            credentials = credentials,
            mode = "download",
            thread_number = thread_number,
            uid = "GET")
        # need these to carry out the transfer
        self.external_id = external_id
        self.req_number = req_number
        self.target_dir = target_dir
        # need these to close the connection
        self.backend_object = backend_object
        self.thread_number = thread_number

    def run(self):
        """Download all the files in the sub file list."""
        try:
            for object_name in self.filelist:
                # external id is the bucket name, add this to the file name
                download_file_path = os.path.join(self.target_dir, object_name)
                # check that the the sub path exists
                sub_path = os.path.split(download_file_path)[0]
                # The "it's better to ask forgiveness method!"
                try:
                    os.makedirs(sub_path)
                except:
                    pass
                self.conn.download_file(
                    self.external_id,
                    object_name,
                    download_file_path
                )
        except SystemExit:
            pass

    def exit(self):
        """Exit the process"""
        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "download",
            thread_number = self.thread_number,
            uid = "GET"
        )


class OS_UploadProcess(multiprocessing.Process):
    """Upload thread for Object Store backend."""
    def setup(self,
              filelist,
              external_id,
              req_number,
              prefix,
              credentials,
              backend_object,
              thread_number
        ):
        self.filelist = filelist
        self.conn = backend_object.connection_pool.find_or_create_connection(
            backend_object,
            req_number = req_number,
            credentials = credentials,
            mode = "upload",
            thread_number = thread_number,
            uid = "PUT")
        # need these to carry out the transfer
        self.req_number = req_number
        self.external_id = external_id
        self.prefix = prefix
        # need these to close the connection
        self.backend_object = backend_object
        self.thread_number = thread_number

    def run(self):
        # we have multiple files so upload them one at once
        try:
            for filename in self.filelist:
                object_name = os.path.relpath(filename, self.prefix)
                self.conn.upload_file(filename,
                                 self.external_id,
                                 object_name)
        except SystemExit:
            pass

    def exit(self):
        """Exit the process"""
        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "upload",
            thread_number = self.thread_number,
            uid = "PUT"
        )


class OS_DeleteProcess(multiprocessing.Process):
    """Delete thread for Object Store backend."""
    def setup(self,
              object_list,
              external_id,
              req_number,
              credentials,
              backend_object,
              thread_number
        ):
        self.object_list = object_list
        self.external_id = external_id
        self.conn = backend_object.connection_pool.find_or_create_connection(
            backend_object,
            req_number = req_number,
            credentials = credentials,
            mode = "upload",
            thread_number = thread_number,
            uid = "DELETE")
        # need these to carry out the transfer
        self.req_number = req_number
        # need these to close the connection
        self.backend_object = backend_object
        self.thread_number = thread_number

    def run(self):
        # we can delete multiple objects, upto a 1000 at a time
        # a maximum of 1000 will be passed by the calling function
        try:
            self.conn.delete_objects(
                Bucket = self.external_id,
                Delete = self.object_list
            )
        except SystemExit:
            pass

    def exit(self):
        """Exit the process"""
        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "upload",
            thread_number = self.thread_number,
            uid = "DELETE"
        )


class ObjectStoreBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets an Object
    Store with S3 HTTP API.
    Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.OS_Settings = read_backend_config(self.get_id())
        self.VERIFY_DIR = self.OS_Settings["VERIFY_DIR"]
        self.ARCHIVE_STAGING_DIR = self.OS_Settings["ARCHIVE_STAGING_DIR"]
        self.connection_pool = ConnectionPool()
        self.download_threads = []
        self.upload_threads = []
        self.delete_threads = []

    def exit(self):
        """Shutdown the backend. Join all the threads."""
        self.connection_pool.close_all_connections()

    def available(self, credentials):
        """Return whether the object store is available or not"""
        try:
            s3c = boto3.client("s3", endpoint_url=self.OS_Settings["S3_ENDPOINT"],
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
            s3c.list_buckets()
            return "available"
        except Exception as e:
            return str(e)

    def monitor(self):
        """Determine which batches have completed."""
        try:
            completed_PUTs = get_completed_puts(self)
            completed_GETs = get_completed_gets(self)
            completed_DELETEs = get_completed_deletes(self)
        except SystemExit:
            return [],[],[]
        except Exception as e:
            raise Exception(e)
        return completed_PUTs, completed_GETs, completed_DELETEs

    def pack_data(self):
        """Should the data be packed into a tarfile for this backend?"""
        return False

    def piecewise(self):
        """For the object store each archive can be uploaded one by one
        and uploads can be resumed."""
        return False

    def create_connection(self, user, workspace, credentials, mode="upload"):
        """Create connection to Object Store, using the supplied credentials"""
        s3c = boto3.client("s3", endpoint_url=self.OS_Settings["S3_ENDPOINT"],
                           aws_access_key_id=credentials['access_key'],
                           aws_secret_access_key=credentials['secret_key'])
        s3c.jdma_user = user
        s3c.jdma_workspace = workspace
        # store a copy of the credentials for the multiprocessing
        s3c.credentials = credentials
        return s3c

    def close_connection(self, conn):
        """Close the connection to the backend.  Do nothing for the object store
        except to close the subprocess connections
        """
        self.connection_pool.close_all_connections()

    def download_files(self, conn, get_req, file_list, target_dir):
        """Download a batch of files from the Object Store to a target
        directory.
        """
        get_req.transfer_id = get_req.migration.external_id
        get_req.save()
        # to take advantage of multiprocesser / threading we divide the download
        # of the files into a number of sub-lists, depending on how many threads
        # we have
        n_files = len(file_list)
        n_threads = int(self.OS_Settings["THREADS"])
        n_files_per_list =  float(n_files) / n_threads

        # keep tabs on the threads created so we can call join later
        self.download_threads = []

        for n in range(0, n_threads):
            start = int(n * n_files_per_list)
            end = int((n+1) * n_files_per_list + 0.5)
            if (end > n_files):
                end = n_files
            subset_filelist = file_list[start:end]
            # we now have a subsets of the files for a single thread, create a
            # process to download each set of files
            thread = OS_DownloadProcess()
            self.download_threads.append(thread)
            # setup the thread with the filelist, bucket_name, target directory
            # this backend and the thread number
            thread.setup(subset_filelist,
                         get_req.migration.external_id,
                         get_req.pk,
                         target_dir,
                         conn.credentials,
                         self,
                         n)
            thread.start()

        for thread in self.download_threads:
            thread.join()

        return len(file_list)

    def __get_new_bucket_name(self, conn):
        """Get the name of the new bucket, using the information in the conn
        object to connect to the object store and the group workspace.
        The name of the bucket is the groupworkspace name appended with the next
        batch number for that groupworkspace."""
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
            # really, we need some ACL to control access to the bucket - limit
            # to users in the group workspace
        except Exception as e:
            bucket_name = None
            raise Exception(str(e))

        return bucket_name

    def upload_files(self, conn, put_req, prefix, file_list):
        """Put an archive, or part of an archive, with paths in file_list onto
        the Object Store."""
        # get the bucket name and save to the external id - if the external id
        # is currently none
        if put_req.migration.external_id is None:
            bucket_name = self.__get_new_bucket_name(conn)
            conn.create_bucket(Bucket=bucket_name)
            put_req.migration.external_id = bucket_name
            put_req.migration.save()

        # to take advantage of multiprocesser / threading we divide the upload
        # of the files into a number of sub-lists, depending on how many threads
        # we have
        n_files = len(file_list)
        n_threads = int(self.OS_Settings["THREADS"])
        n_files_per_list =  float(n_files) / n_threads

        # keep tabs on the threads created so we can call join later
        self.upload_threads = []

        for n in range(0, n_threads):
            start = int(n * n_files_per_list)
            end = int((n+1) * n_files_per_list + 0.5)
            if (end > n_files):
                end = n_files
            subset_filelist = file_list[start:end]
            # we now have a subsets of the files for a single thread, create a
            # process to upload each set of files
            thread = OS_UploadProcess()
            self.upload_threads.append(thread)
            # setup the thread with the filelist, bucket_name, target directory
            # this backend and the thread number
            thread.setup(subset_filelist,
                         put_req.migration.external_id,
                         put_req.pk,
                         prefix,
                         conn.credentials,
                         self,
                         n)
            thread.start()

        for thread in self.upload_threads:
            thread.join()

        return len(file_list)


    def delete_batch(self, conn, del_req, batch_id):
        """Delete a whole batch from the object store"""
        # we have to delete the bucket and all its contents
        # s3 client only allows for 1000 keys to be returned using the
        # list_object_v2 method.  We use continuation token to continue to
        # get the name of all objects in a loop
        kwargs = {'Bucket': batch_id}
        continuation = True
        # keep tabs on the threads created so we can call join later
        self.delete_threads = []
        n_threads = int(self.OS_Settings["THREADS"])
        current_thread = 0
        while continuation:
            # check whether we should join any threads
            if (len(self.delete_threads) >= n_threads):
                for thread in self.delete_threads:
                    thread.join()
                # reset the delete threads
                self.delete_threads = []
                current_thread = 0
            # list objects
            response = conn.list_objects_v2(**kwargs)
            # get each object name in turn and delete it
            try:
                object_list = []
                for obj in response['Contents']:
                    object_name = obj['Key']
                    object_list.append({'Key' : object_name})
                # we now have a subses of the files for a single thread, create a
                # process to delete each set of files
                thread = OS_DeleteProcess()
                self.delete_threads.append(thread)
                # setup the thread with the filelist, bucket_name, target directory
                # this backend and the thread number
                delete_dict = {'Objects' : object_list}
                thread.setup(delete_dict,
                             del_req.migration.external_id,
                             del_req.pk,
                             conn.credentials,
                             self,
                             current_thread)
                current_thread += 1
                thread.start()

            except KeyError:
                pass

            try:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            except KeyError:
                continuation = False

        # join any left over threads
        for thread in self.delete_threads:
            thread.join()

        # delete the bucket
        try:
            conn.delete_bucket(Bucket=batch_id)
        except:
            pass

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
        return int(self.OS_Settings["OBJECT_SIZE"])

    def maximum_object_count(self):
        """Maximum number of objects in an archive"""
        return (int(self.OS_Settings["OBJECT_COUNT"]))
