import os
import stat
import datetime
import hashlib
import logging
import math
from collections import namedtuple

from jasmin_ldap.query import *
import jdma_site.settings as settings

FileInfo = namedtuple('FileInfo',
                      ['filepath', 'size', 'digest', 'unix_user_id',
                       'unix_group_id', 'unix_permission', 'is_dir'],
                       verbose=False)

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
    Info is: size, SHA-256 digest, unix-uid, unix-gid, unix-permissions, dir?"""
    # get the permissions etc. of the original file
    fstat = os.stat(filepath)
    size = fstat.st_size
    # calc SHA256 digest
    if stat.S_ISDIR(fstat.st_mode):
        digest = 0
        is_dir = True
    else:
        digest = calculate_digest(filepath)
        is_dir = False
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
        unix_permission,
        is_dir
    )


def mark_migration_failed(mig_req, failure_reason, e_inst=None, upload_mig=True):
    from jdma_control.models import Migration, MigrationRequest
    logging.error(failure_reason)
    mig_req.stage = MigrationRequest.FAILED
    mig_req.failure_reason = str(failure_reason)
    # lock the migration request so it can't be retried
    mig_req.locked = True
    # only reset these if the upload migration (PUT | MIGRATE) fails
    # if a GET fails then the migration is unaffected
    if upload_mig:
        mig_req.migration.stage = Migration.FAILED
        #mig_req.migration.external_id = None
        mig_req.migration.save()
    mig_req.save()
    # raise exception if debug
    debug = True
    if debug and e_inst != None:
        raise Exception(e_inst)


def sizeof_fmt(num):
    """Human friendly file size"""
    unit_list = (['bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB'],
                 [0, 0, 1, 1, 1, 1, 1])
    if num > 1:
        exponent = min(int(math.log(num, 1024)), len(unit_list[0]) - 1)
        quotient = float(num) / 1024**exponent
        unit = unit_list[0][exponent]
        num_decimals = unit_list[1][exponent]
        format_string = '{:>5.%sf} {}' % (num_decimals)
        return format_string.format(quotient, unit)
    elif num == 1:
        return '1 byte'
    else:
        return '0 bytes'


def get_archive_set_from_get_request(gr):
    # if the filelist for the GET request is not None then we have to determine
    # which archives to download
    from jdma_control.models import MigrationArchive
    # if the filelist is "None"
    if not gr.filelist:
        archive_set = MigrationArchive.objects.filter(
            migration=gr.migration,
        ).order_by('pk')
    # if the filelist is the common_path (i.e. the user has selected a directory
    # to migrate) then return all the archives in the set
    elif len(gr.filelist) > 0 and gr.filelist[0] == gr.migration.common_path:
        archive_set = MigrationArchive.objects.filter(
            migration=gr.migration,
        ).order_by('pk')
    else:
    # otherwise build a list of filepaths with the common_path removed
    # (this is the way they are stored in the archive record)
        filelist_no_cp = [x.replace(gr.migration.common_path, "")
                           for x in gr.filelist]
        # get the archive set
        archive_set = MigrationArchive.objects.filter(
            migration=gr.migration,
            migrationfile__path__in=filelist_no_cp
        ).order_by('pk')
    # make sure we loop over all the archives in the (sub)set
    st_arch = gr.last_archive
    n_arch = archive_set.count()
    return archive_set, st_arch, n_arch


def get_verify_dir(backend_object, pr):
    verify_dir = os.path.join(
        backend_object.VERIFY_DIR,
        "verify_{}_{}".format(
            backend_object.get_id(),
            pr.migration.external_id
        )
    )
    return verify_dir


def get_staging_dir(backend_object, pr):
    staging_dir = os.path.join(
        backend_object.ARCHIVE_STAGING_DIR,
        pr.migration.get_id()
    )
    return staging_dir


def get_download_dir(backend_object, gr):
    download_dir = os.path.join(
        backend_object.ARCHIVE_STAGING_DIR,
        "download_{}_{}".format(
            backend_object.get_id(),
            gr.migration.external_id
        )
    )
    return download_dir
