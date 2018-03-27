import os
import datetime
import hashlib
import logging
import math
from collections import namedtuple

from jasmin_ldap.query import *
import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest

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


def mark_migration_failed(mig_req, failure_reason, upload_mig=True):
    logging.error(failure_reason)
    mig_req.stage = MigrationRequest.FAILED
    mig_req.failure_reason = failure_reason
    # lock the migration request so it can't be retried
    mig_req.locked = True
    # only reset these if the upload migration (PUT | MIGRATE) fails
    # if a GET fails then the migration is unaffected
    if upload_mig:
        mig_req.migration.stage = Migration.FAILED
        #mig_req.migration.external_id = None
        mig_req.migration.save()
    mig_req.save()

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
