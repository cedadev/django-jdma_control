"""Class for a JASMIN Data Migration App backend that targets a FTP server
   using the Python ftplib

   Creating a migration on a ftp server consists of the following
   operations:
    1.  Create a directory for the group workspace and current batch id, as an
        identifier
    2.  Upload a tarfile archive to the directory as part of the migrations

   """
import os

import ftplib
from django.db.models import Q

from jdma_control.backends.Backend import Backend
from jdma_control.scripts.config import read_backend_config
from jdma_control.backends.ConnectionPool import ConnectionPool
from jdma_control.backends import AES_tools
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import get_verify_dir, get_staging_dir
import jdma_site.settings as settings

import multiprocessing
import signal

def get_completed_puts(backend_object):
    """Get all the completed puts for the FTP backend"""
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, Migration, StorageQuota
    # get the storage id
    storage_id = StorageQuota.get_storage_index("ftp")
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
            ftp = ftplib.FTP(host=backend_object.FTP_Settings["FTP_ENDPOINT"],
                             user=credentials['username'],
                             passwd=credentials['password'])
            # loop over each archive in the migration
            archive_set = pr.migration.migrationarchive_set.order_by('pk')
            # counter for number of uploaded archives
            n_up_arch = 0
            for archive in archive_set:
                # get the list of files for this archive
                file_list = archive.get_file_names()['FILE']
                n_files = 0
                for file_path in file_list['FILE']:
                    # object name is the file_path, without the gws prefix
                    object_name = (pr.migration.external_id +
                                   "/" + file_path)
                    # enforce switch to binary (images here, but that doesn't
                    # matter)
                    ftp.voidcmd('TYPE I')
                    try:
                        fsize = ftp.size(object_name)
                        if fsize is not None:
                            n_files += 1
                    except:
                        pass
                # check if all files uploaded and then inc archive
                if n_files == len(file_list):
                    n_up_arch += 1

            if n_up_arch == pr.migration.migrationarchive_set.count():
                completed_PUTs.append(pr.migration.external_id)

            ftp.quit()
        except Exception as e:
            raise Exception(e)

    return completed_PUTs

def get_completed_gets(backend_object):
    # This is the same as ObjectStoreBackend::get_completed_gets
    # That might change in the future, though
    # avoiding a circular dependency
    from jdma_control.models import MigrationRequest, StorageQuota
    from jdma_control.models import MigrationFile, MigrationArchive
    # get the storage id
    storage_id = StorageQuota.get_storage_index("ftp")

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
                    staging_dir = get_staging_dir(backend_object, gr)
                else:
                    staging_dir = gr.target_path
            # now loop over each file in the archive
            n_completed_files = 0
            file_name_list = archive.get_file_names(
                filelist = gr.filelist
            )
            for file_name in file_name_list['FILE']:
                file_path = os.path.join(staging_dir, file_name)
                try:
                    # just rely on exception thrown if file does not exist yet
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
    storage_id = StorageQuota.get_storage_index("ftp")
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
            ftp = ftplib.FTP(host=backend_object.FTP_Settings["FTP_ENDPOINT"],
                             user=credentials['username'],
                             passwd=credentials['password'])
            # if the external_id directory has been deleted then the
            # deletion has completed
            dir_list = ftp.mlsd("/")
            found = False
            for d in dir_list:
                # check if directory and groupworkspace name is in directory
                if d[1]['type'] == 'dir' and dr.migration.external_id in d[0]:
                    found = True
                    break
            if not found:
                completed_DELETEs.append(dr.migration.external_id)

        except Exception as e:
            raise Exception(e)
    return completed_DELETEs


class FTP_DownloadProcess(multiprocessing.Process):
    """Download thread for FTP backend."""
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
        # change the working directory to the external batch id
        try:
            self.conn.cwd("/" + self.external_id)
            for filename in self.filelist:
                # external id is the bucket name, add this to the file name
                download_file_path = os.path.join(self.target_dir, filename)
                # check that the the sub path exists
                sub_path = os.path.split(download_file_path)[0]
                # The "it's better to ask forgiveness method!"
                try:
                    os.makedirs(sub_path)
                except:
                    pass
                # open the download file
                fh = open(download_file_path, 'wb')
                self.conn.retrbinary("RETR " + filename, fh.write)
                fh.close()
        except SystemExit:
            pass


    def exit(self):
        """FTP Download exit handler."""
        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "download",
            thread_number = self.thread_number,
            uid = "GET"
        )


class FTP_UploadProcess(multiprocessing.Process):
    """Upload thread for FTP backend."""
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
        # upload the multiple files in the file_list
        # change the working directory to the external batch id
        try:
            self.conn.cwd("/" + self.external_id)

            for filename in self.filelist:
                # change to the directory where the file will be deposited
                ftp_file_name = os.path.relpath(filename, self.prefix)
                # open the file from the archive_path in binary mode
                fh = open(filename, 'rb')
                self.conn.storbinary("STOR " + ftp_file_name, fh)
                fh.close()
        except SystemExit:
            pass

    def exit(self):
        """FTP Upload exit handler."""
        if settings.TESTING:
            print ("   Exit FTP_UploadProcess")

        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "upload",
            thread_number = self.thread_number,
            uid = "PUT"
        )


class FTP_DeleteProcess(multiprocessing.Process):
    """Delete thread for FTP backend."""
    def setup(self,
              filelist,
              external_id,
              req_number,
              credentials,
              backend_object,
              thread_number
        ):
        self.filelist = filelist
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
        self.conn.cwd("/" + self.external_id)
        try:
            for filepath in self.filelist:
                # remove the file
                try:
                    conn.delete(filepath)
                except ftplib.error_perm as e:
                    # handle directory already created
                    if not '550' in e.args[0]:
                        raise Exception(e)
                    else:
                        continue
        except SystemExit:
            pass

    def exit(self):
        """FTP Delete exit handler."""
        if settings.TESTING:
            print ("   Exit FTP_DeleteProcess")

        self.backend_object.connection_pool.close_connection(
            self.backend_object,
            req_number = self.req_number,
            mode = "upload",
            thread_number = self.thread_number,
            uid = "DELETE"
        )


class FTPBackend(Backend):
    """Class for a JASMIN Data Migration App backend which targets a FTP server
    with Python ftplib .
    Inherits from Backend class and overloads inherited functions."""

    def __init__(self):
        """Need to set the verification directory and logging"""
        self.FTP_Settings = read_backend_config(self.get_id())
        self.VERIFY_DIR = self.FTP_Settings["VERIFY_DIR"]
        self.ARCHIVE_STAGING_DIR = self.FTP_Settings["ARCHIVE_STAGING_DIR"]
        self.connection_pool = ConnectionPool()
        self.download_threads = []
        self.upload_threads = []
        self.delete_threads = []

    def exit(self):
        """Shutdown the backend. Join all the threads."""
        # join the threads
        if settings.TESTING:
            print ("Exit FTPBackend")
        for dt in self.download_threads:
            dt.join()
            dt.exit()
        for ut in self.upload_threads:
            ut.join()
            ut.exit()
        for dt in self.delete_threads:
            dt.join()
            dt.exit()

    def available(self, credentials):
        """Return whether the backend storage is avaliable at the moment
        - i.e. switched on or not!
        """
        try:
            conn = ftplib.FTP(host=self.FTP_Settings["FTP_ENDPOINT"],
                              user=credentials['username'],
                              passwd=credentials['password'])
            conn.quit()
            return True
        except:
            return False

    def monitor(self):
        """Monitor the external storage, return which requests have completed"""
        try:
            completed_PUTs = get_completed_puts(self)
            completed_GETs = get_completed_gets(self)
            completed_DELETEs = get_completed_deletes(self)
            # pause if no transfers
        except SystemExit:
            return [], [], []
        except Exception as e:
            raise Exception(e)
        return completed_PUTs, completed_GETs, completed_DELETEs

    def pack_data(self):
        """Should the data be packed into a tarfile for this backend?"""
        return False

    def piecewise(self):
        """Should the data be uploaded piecewise (archive by archive) or
        all at once?"""
        return True

    def create_connection(self, user, workspace, credentials, mode="upload"):
        """Create a connection to the FTP server, using the supplied credentials.
        """
        ftp = ftplib.FTP(host=self.FTP_Settings["FTP_ENDPOINT"],
                         user=credentials['username'],
                         passwd=credentials['password'])
        ftp.jdma_user = user
        ftp.jdma_workspace = workspace
        ftp.credentials = credentials
        return ftp

    def close_connection(self, conn):
        """Close the connection to the backend"""
        # close the connections in the connection pool
        self.connection_pool.close_all_connections()
        conn.quit()

    def download_files(self, conn, get_req, file_list, target_dir):
        """Download a batch of files from the FTP server to a target directory
        """
        get_req.transfer_id = get_req.migration.external_id
        get_req.save()

        # now do the download via a multiprocess Process
        n_files = len(file_list)
        n_threads = int(self.FTP_Settings["THREADS"])
        n_files_per_list = float(n_files) / n_threads

        # keep tabs on the threads created so we can call join later
        self.download_threads = []

        for n in range(0, n_threads):
            start = int(n * n_files_per_list)
            end = int((n+1) * n_files_per_list + 0.5)
            if (end > n_files):
                end = n_files
            subset_filelist = file_list[start:end]
            # we now have a subsets of the files for a single thread, create a
            # process to upload each set of files
            thread = FTP_DownloadProcess()
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

    def __get_new_directory_name(self, conn):
        """Get the name of the new directory on the FTP server
        The name of the directory is the groupworkspace name appended with the
        next batch number for that groupworkspace."""
        gws_dir_prefix = "gws-" + conn.jdma_workspace + "-"
        try:
            # get a list of the directories in the top-level directory
            dir_list = conn.mlsd("/")
            batch_id = 0
            for d in dir_list:
                # check if directory and groupworkspace name is in directory
                if d[1]['type'] == 'dir' and gws_dir_prefix in d[0]:
                    # get the id
                    c_id = int(d[0][len(gws_dir_prefix):])
                    # check whether this is greatest batch id and create
                    # one larger if it is
                    if c_id >= batch_id:
                        batch_id = c_id + 1

            # create the directory name: format c_id to 10 digits
            dir_name = "{}{:010}".format(gws_dir_prefix, batch_id)
        except Exception as e:
            dir_name = None
            raise Exception(str(e))

        return dir_name

    def __get_list_of_directories(self, file_list, prefix=""):
        # loop through the paths, get the file name and create a list of
        # directories that need to be created
        dir_list = []
        for archive_path in file_list:
            dir_path = os.path.dirname(os.path.relpath(archive_path, prefix))
            # split on '/' and add to the list of directories
            dir_path_split = dir_path.split("/")
            for p in range(0, len(dir_path_split)+1):
                sub_path = "/".join(dir_path_split[0:p])
                if not sub_path in dir_list and len(sub_path) > 0:
                    dir_list.append(sub_path)
        return dir_list

    def upload_files(self, conn, put_req, prefix, file_list):
        """Upload a list of files to the FTP server"""
        # get the directory name and save to the external id - if the external
        # id is currently none
        if put_req.migration.external_id is None:
            # create a new name
            dir_name = self.__get_new_directory_name(conn)
            # make the dir - ensure we are at the root of the groupworkspace
            conn.cwd("/")
            conn.mkd(dir_name)
            put_req.migration.external_id = dir_name
            put_req.migration.save()

        # get a list of just the directories, not the files
        dir_list = self.__get_list_of_directories(file_list, prefix)
        conn.cwd("/" + put_req.migration.external_id)

        # create the directories from this directory list
        for path in dir_list:
            try:
                conn.mkd(path)
            except ftplib.error_perm as e:
                # handle directory already created
                if not '550' in e.args[0]:
                    raise Exception(e)
                else:
                    continue

        # now do the upload via a multiprocess Process
        n_files = len(file_list)
        n_threads = int(self.FTP_Settings["THREADS"])
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
            thread = FTP_UploadProcess()
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
        """Delete a batch from the FTP server"""
        del_req.transfer_id = del_req.migration.external_id
        del_req.save()

        archive_set = del_req.migration.migrationarchive_set.order_by('pk')

        # change the working directory to the external batch id
        conn.cwd("/" + del_req.migration.external_id)
        dirs_to_delete = []
        file_list = []
        for archive in archive_set:
            # get the list of files for this archive
            file_list.extend(archive.get_file_names()['FILE'])
            # get the directories those files belong to and add to the global
            # list of directories
            dir_list = self.__get_list_of_directories(file_list)
            dirs_to_delete.extend(dir_list)

        # now do the download via a multiprocess Process
        n_files = len(file_list)
        n_threads = int(self.FTP_Settings["THREADS"])
        n_files_per_list =  float(n_files) / n_threads
        # keep tabs on the threads created so we can call join later
        self.delete_threads = []

        for n in range(0, n_threads):
            start = int(n * n_files_per_list)
            end = int((n+1) * n_files_per_list + 0.5)
            if (end > n_files):
                end = n_files
            subset_filelist = file_list[start:end]
            # we now have a subsets of the files for a single thread, create a
            # process to upload each set of files
            thread = FTP_DeleteProcess()
            self.delete_threads.append(thread)
            # setup the thread with the filelist, bucket_name, target directory
            # this backend and the thread number
            thread.setup(subset_filelist,
                         del_req.migration.external_id,
                         del_req.pk,
                         conn.credentials,
                         self,
                         n)
            thread.start()

        for thread in self.delete_threads:
            thread.join()

        # delete the directories that have been collected - do it backwards:
        for del_dir in dirs_to_delete[::-1]:
            try:
                conn.rmd(del_dir)
            except ftplib.error_perm as e:
                # handle directory already created
                if not '550' in e.args[0]:
                    raise Exception(e)

        # delete toplevel directory
        conn.cwd("/")
        try:
            conn.rmd(del_req.migration.external_id)
        except ftplib.error_perm as e:
            # handle directory already created
            if not '550' in e.args[0]:
                raise Exception(e)

    # permissions / quota
    def user_has_put_permission(self, conn):
        """Does the user have permission to write to the workspace
        on the storage device?
        """
        # groupworkspace permission
        gws_permission = Backend._user_has_put_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )
        return gws_permission

    def user_has_get_permission(self, batch_id, conn):
        """Does the user have permission to get the migration request from the
        storage device?"""
        from jdma_control.models import Migration

        # groupworkspace permission
        gws_permission = Backend._user_has_get_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace
        )
        # get the batch name and try to change to that directory
        migration = Migration.objects.get(pk=int(batch_id))
        try:
            conn.cwd(migration.external_id)
            ftp_permission = True
        except:
            ftp_permission = False

        return gws_permission & ftp_permission

    def user_has_delete_permission(self, batch_id, conn):
        """Determine whether the user has the permission to delete the batch
        given by batch_id in the workspace.
        """
        from jdma_control.models import Migration

        # groupworkspace permission
        gws_permission = Backend._user_has_delete_permission(
            self, conn.jdma_user, conn.jdma_workspace.workspace, batch_id
        )

        # get the batch name and try to change to that directory
        migration = Migration.objects.get(pk=int(batch_id))
        try:
            conn.cwd(migration.external_id)
            ftp_permission = True
        except:
            ftp_permission = False

        return gws_permission & ftp_permission

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
        return "FTP"     # get the name for error messages

    def get_id(self):
        return "ftp"

    def required_credentials(self):
        """Get the keys of the required credentials to use this backend.
           These keys, along with their values, will be stored in a hidden file
           in the user's home directory.
           They will be encrypted and stored in the MigrationRequest so that the
           daemon processes can carry out the Migrations on behalf of the user.
        """
        return ["username", "password"]

    def minimum_object_size(self):
        """The minimum recommended size for a file on this external storage
        medium.
        This should be overloaded in the inherited (super) classes
        """
        # in bytes - assume this is filesystem, so "optimum" is 32MB
        return self.FTP_Settings["OBJECT_SIZE"]

    def maximum_object_count(self):
        """Maximum number of objects in an archive"""
        return (int(self.FTP_Settings["OBJECT_COUNT"]))
