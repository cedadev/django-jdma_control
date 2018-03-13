"""Functions to transition a migration request from:
ON_DISK -> PUT_PENDING     - locks the directory for migration by changing
   the owner to root / jdma user
ON_STORAGE -> GET_PENDING  - create the target directory, and lock it by
   changing the owner again

This is a simple program that is designed to be run at high-frequency,
e.g. every minute even.
"""

import datetime
import logging
import subprocess
import os
import hashlib
from collections import namedtuple
from operator import attrgetter

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.models import MigrationArchive, MigrationFile
from jasmin_ldap.core import *
from jasmin_ldap.query import *
from jdma_control.backends.Backend import get_backend_object

FileInfo = namedtuple('FileInfo',
                      ['filepath', 'size', 'digest', 'unix_user_id',
                       'unix_group_id', 'unix_permission'], verbose=False)


def calculate_digest(filename):
    # Calculate the hex digest of the file, using a buffer
    BUFFER_SIZE = 256 * 1024  # (256KB) - adjust this

    # create a sha256 object
    sha256 = hashlib.sha256()

    # read through the file
    with open(filename, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)
            if not data:  # EOF
                break
            sha256.update(data)
    return "{0}".format(sha256.hexdigest())


def setup_logging(module_name):
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
    log_fname = log_path + "/" + module_name + "_" + date_string

    logging.basicConfig(filename=log_fname, level=logging.DEBUG)


def get_file_info_tuple(filepath, user_name, conn):
    """Get all the info for a file, and return in a tuple.
    Info is: size, SHA-256 digest, unix-uid, unix-gid, unix-permissions"""
    # get the permissions etc. of the original file
    fstat = os.stat(filepath)
    size = fstat.st_size
    # calc SHA256 digest
    if os.path.isdir(filepath):
        digest = 0
    else:
        digest = calculate_digest(filepath)
    # get the unix user id owner of the file - use LDAP now
    # query to find username with uidNumber matching fstat.st_uid - default to
    # user
    query = Query(
        conn,
        base_dn=settings.JDMA_LDAP_BASE_USER
    ).filter(uidNumber=fstat.st_uid)
    if len(query) == 0 or len(query[0]) == 0:
        unix_user_id = user_name
    else:
        unix_user_id = query[0]["uid"][0]

    # query to find group with gidNumber matching fstat.gid - default to users
    # group
    query = Query(
        conn,
        base_dn=settings.JDMA_LDAP_BASE_GROUP
    ).filter(gidNumber=fstat.st_gid)
    if len(query) == 0 or len(query[0]) == 0:
        unix_group_id = "users"
    else:
        unix_group_id = query[0]["cn"][0]

    # get the unix permissions
    unix_permission = "{}".format(oct(fstat.st_mode))
    unix_permission = int(unix_permission[-3:])
    return FileInfo(
        filepath,
        size,
        digest,
        unix_user_id,
        unix_group_id,
        unix_permission
    )


def lock_migration(pr, conn):
    """Move this to it's own function so that it opens the possibility of
    threading later on
    """
    # loop over the files or directories - copy full paths and other info
    # about the file into a list
    fileinfos = []
    for fd in pr.migration.filelist:
        # check whether it's a directory: walk if it is
        if os.path.isdir(fd):
            # create the file list of all the files and directories under
            # the original directory
            # don't follow symbolic links!
            user_file_list = os.walk(fd, followlinks=False)
            for root, dirs, files in user_file_list:
                # files
                for fl in files:
                    # get the full path and append to the list
                    filepath = os.path.join(root, fl)
                    # get the info for the file
                    file_info = get_file_info_tuple(
                        filepath,
                        pr.migration.user.name,
                        conn
                    )
                    # append
                    fileinfos.append(file_info)
                # directories
                for dl in dirs:
                    filepath = os.path.join(root, fl)
                    # get the info for the file
                    file_info = get_file_info_tuple(
                        filepath,
                        pr.migration.user.name,
                        conn
                    )
                    # append a directory
                    if os.path.isdir((file_info.filepath)):
                        fileinfos.append(file_info)
                # root
                file_info = get_file_info_tuple(
                    root,
                    pr.migration.user.name,
                    conn
                )
                # append the root
                if os.path.isdir(file_info.filepath):
                    fileinfos.append(file_info)
            # 1. change the owner of the directory to be root
            # 2. change the read / write permissions to be user-only
            subprocess.call(["/usr/bin/sudo", "/bin/chown", "-R", "root:root", fd])
            subprocess.call(["/usr/bin/sudo", "/bin/chmod", "-R", "700", fd])

        else:
            # get the info for the file
            file_info = get_file_info_tuple(
                fd,
                pr.migration.user.name,
                conn
            )
            # append to the list of files
            fileinfos.append(file_info)
            # 1. change the owner of the file to be root
            # 2. change the read / write permissions to be user-only
            subprocess.call(["/usr/bin/sudo", "/bin/chown", "root:root", fd])
            subprocess.call(["/usr/bin/sudo", "/bin/chmod", "700", fd])

    # sort the fileinfos based on size using attrgetter
    # this will group all the small files together
    # we will take files from the back, so set descending to True
    fileinfos.sort(key=attrgetter('size'), reverse=True)

    # get the backend object minimum size
    backend_object = get_backend_object(pr.migration.storage.get_name())

    # keep adding files to MigrationArchives until there are none left
    # (when current file < 0)
    n_current_file = len(fileinfos) - 1

    while n_current_file >= 0:
        # create a new MigrationArchive
        mig_arc = MigrationArchive()
        # assign the migration, copy from the MigrationRequest
        mig_arc.migration = pr.migration
        # save the migration archive
        mig_arc.save()

        # now create the files - while there are files left and the current
        # archive size is less than the minimum object size for the backend
        current_size = 0
        while (n_current_file >= 0 and
                current_size < backend_object.minimum_object_size()):
            # create the migration file using the fileinfo pointed to by
            # n_current_file
            mig_file = MigrationFile()
            fileinfo = fileinfos[n_current_file]
            # fill in the details
            mig_file.path = fileinfo.filepath
            mig_file.size = fileinfo.size
            mig_file.digest = fileinfo.digest
            mig_file.unix_user_id = fileinfo.unix_user_id
            mig_file.unix_group_id = fileinfo.unix_group_id
            mig_file.unix_permission = fileinfo.unix_permission
            mig_file.archive = mig_arc

            # add the size to the current archive size
            current_size += fileinfos[n_current_file].size
            # go to the next file (going backwards through a descending
            # sorted list remember!)
            n_current_file -= 1
            # save the Migration File
            mig_file.save()
            logging.info("PUT: Added file: " + mig_file.path)

    # set the MigrationRequest stage to be PUT_PENDING and the
    # Migration stage to be PUTTING
    pr.stage = MigrationRequest.PUT_PENDING
    pr.migration.stage = Migration.PUTTING
    pr.migration.save()
    pr.save()


def lock_put_filelists():
    """Lock the directories that are going to be put to external storage.
       Also build the MigrationFiles entries from walking the directories
       This is to ensure that the user doesn't write any more data to them
       while the external storage write is ongoing.
    """
    # get the list of PUT requests
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.PUT_START)
    )
    # create the required ldap server pool, do this just once to
    # improve performance
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY,
                              settings.JDMA_LDAP_REPLICAS)
    conn = Connection.create(ldap_servers)
    # for each PUT request get the Migration and determine if the type of the
    # Migration is ON_DISK
    for pr in put_reqs:
        lock_migration(pr, conn)
    conn.close()


def lock_get_directories():
    """Lock the directories that the targets for recovering data from external
    storage.  This is to ensure that there aren't any filename conflicts.
    """
    # get the list of GET requests
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(stage=MigrationRequest.GET_START)
    )
    # for each GET request get the Migration and determine if the type of the
    # Migration is ON_TAP
    for gr in get_reqs:
        # if it's on external storage then:
        # 1. Make the directory if it doesn't exist
        # 2. change the owner of the directory to be root
        # 3. change the read / write permissions to be user-only
        if not os.path.isdir(gr.target_path):
            subprocess.call(["/usr/bin/sudo", "/bin/mkdir",
                             gr.target_path])
        subprocess.call(["/usr/bin/sudo", "/bin/chown", "root:root",
                        gr.target_path])
        subprocess.call(["/usr/bin/sudo", "/bin/chmod", "700",
                        gr.target_path])
        # set the migration stage to be GET_PENDING
        gr.stage = MigrationRequest.GET_PENDING
        gr.save()
        logging.info("GET: Locked directory: " + gr.target_path)
        logging.info((
            "Transition: request ID: {} ON_STORAGE->GET_PENDING"
        ).format(gr.pk))


def run():
    """Entry point for the Django script run via ``./manage.py runscript``
    """
    setup_logging(__name__)
    lock_put_filelists()
    lock_get_directories()
