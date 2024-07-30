import os
import stat
import datetime
import hashlib
import zlib
import logging
import subprocess
import math
from collections import namedtuple

#import jdma_site.settings as settings
import socket

FileInfo = namedtuple('FileInfo',
                      ['filepath', 'size', 'digest', 'digest_format',
                       'unix_user_id', 'unix_group_id', 'unix_permission',
                       'ftype', 'link_target'])

def split_args(args):
    # split args that are in the form somekey=somevalue into a dictionary
    arg_dict = {}
    for a in args:
        try:
            split_args = a.split("=")
            arg_dict[split_args[0]] = split_args[1]
        except:
            raise Exception("Error in arguments")
    return arg_dict

def calculate_digest_sha256(filename):
    # Calculate the hex digest of the file, using a buffer
    BUFFER_SIZE = 1024 * 1024  # (1MB) - adjust this

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

def calculate_digest_adler32(filename):
    # Calculate the hex digest of the file, using a buffer
    BUFFER_SIZE = 1024 * 1024  # (1MB) - adjust this
    # read through the file
    prev = 0
    with open(filename, 'rb') as file:
        while True:
            data = file.read(BUFFER_SIZE)
            if not data:  # EOF
                break
            cur = zlib.adler32(data, prev)
            prev = cur
    return "{0}".format(hex(prev & 0xffffffff))

def get_file_info_tuple(filepath):
    """Get all the info for a file, and return in a tuple.
    Info is: size, SHA-256 digest, unix-uid, unix-gid, unix-permissions, dir?"""
    # get the permissions etc. of the original file
    fstat = os.stat(filepath, follow_symlinks=False)
    size = fstat.st_size
    link_target = ""
    # calc digest
    if stat.S_ISLNK(fstat.st_mode):
        digest = 0
        digest_format = ""
        ftype = "LINK"
        # get the link location
        link_target = os.path.abspath(os.path.realpath(filepath))
    elif stat.S_ISDIR(fstat.st_mode):
        digest = 0
        digest_format = ""
        ftype = "DIR"
    else:
        # default to adler32 now for speeed
        digest = calculate_digest_adler32(filepath)
        digest_format = "ADLER32"
        ftype = "FILE"
    # get the unix user id owner of the file - just use the raw value and store
    # as integer now
    unix_user_id = fstat.st_uid
    # get unix group id - just use the raw value and store as integer
    unix_group_id = fstat.st_gid
    # get the unix permissions
    unix_permission = "{}".format(oct(fstat.st_mode))
    # NRM - 04/01/2021 - retain the "sticky" bit
    unix_permission = int(unix_permission[-4:])
    return FileInfo(
        filepath,
        size,
        digest,
        digest_format,
        unix_user_id,
        unix_group_id,
        unix_permission,
        ftype,
        link_target
    )


def restore_owner_and_group(mig, target_path, filelist=[]):
    # change the owner, group and permissions of the file to match that
    # of the original from the user query

    # start at the last_archive so that interrupted uploads can be resumed
    st_arch = 0
    n_arch = mig.migrationarchive_set.count()
    archive_set = mig.migrationarchive_set.order_by('pk')
    logging.info(
        "Changing owner and file permissions on migration {} {}".format(
        mig.pk, mig.label
    ))
    for arch_num in range(st_arch, n_arch):
        # determine which archive to change the permissions for
        archive = archive_set[arch_num]

        # get the migration files in the archive
        # order by file type, so the "D"IRS are created  before the "F"iles,
        # which are created before the "L"INKS
        mig_files = archive.migrationfile_set.all().order_by("ftype")
        for mig_file in mig_files:
            if not mig_file:
                continue
            # get the uid and gid
            uidNumber = mig_file.unix_user_id
            gidNumber = mig_file.unix_group_id
            # check if in the filelist, if neccessary
            if (not(filelist == [] or filelist == None) 
                and mig_file.path not in filelist):
                continue

            # form the file path
            file_path = os.path.join(
                target_path,
                mig_file.path
            )
            # determine whether it's a link
            link = False

            # if it's a directory then recreate the directory
            if mig_file.ftype == "DIR":
                logging.debug(
                    "Created directory: {}".format(
                    file_path
                ))
                os.makedirs(file_path, exist_ok=True)
            elif mig_file.ftype == "LNAS":
                ln_src_path = mig_file.link_target
                ln_tgt_path = file_path
                link = True
            elif mig_file.ftype == "LNCM":
                ln_src_path = os.path.join(target_path, mig_file.link_target)
                ln_tgt_path = file_path
                link = True

            if link:
                # remove the symlink if it exists
                try:
                    os.symlink(ln_src_path, ln_tgt_path)
                    logging.debug(
                        "Created symlink from {} to {}".format(
                        ln_src_path, ln_tgt_path
                    )
                )
                except OSError as e:
                    if e.errno == os.errno.EEXIST:
                        os.unlink(ln_tgt_path)
                        os.symlink(ln_src_path, ln_tgt_path)
                        logging.debug(
                            "Deleted then created symlink from {} to {}".format(
                                ln_src_path, ln_tgt_path
                            )
                        )
                except Exception as e:
                    logging.error(
                        "Could not create symlink from {} to {} : {}".format(
                            ln_src_path, ln_tgt_path, str(e)
                        )
                    )

            # Note that, if there is a filelist then the files may not exist
            # However, to successfully restore any parent directories that may
            # be created, we have to attempt to restore all of the files in the
            # archive.  We'll do this by checking the filepath
            if os.path.exists(file_path):
                # change the directory owner / group
                subprocess.call(
                    ["/usr/bin/sudo",
                     "/bin/chown",
                     str(uidNumber)+":"+str(gidNumber),
                     file_path]
                )

                # change the permissions back to the original
                subprocess.call(
                    ["/usr/bin/sudo",
                     "/bin/chmod",
                     str(mig_file.unix_permission),
                     file_path]
                )
                logging.debug(
                    "Changed owner and file permissions for file {}".format(
                        file_path
                    )
                )
            else:
                logging.error(
                    "Could not change owner and permissions on file {}".format(
                        file_path
                    )
                )
    # restore the target_path
    # change the directory owner / group
    if target_path:
        subprocess.call(
            ["/usr/bin/sudo",
             "/bin/chown",
             str(mig.common_path_user_id)+":"+str(mig.common_path_group_id),
             target_path]
        )

        # change the permissions back to the original
        subprocess.call(
            ["/usr/bin/sudo",
             "/bin/chmod",
             str(mig.common_path_permission),
             target_path]
        )


def mark_migration_failed(mig_req, failure_reason, e_inst=None, upload_mig=True):
    from jdma_control.models import Migration, MigrationRequest
    # lock the migration request so it can't be retried
    if not mig_req.lock():
        return
    logging.error(failure_reason)
    mig_req.stage = MigrationRequest.FAILED
    mig_req.failure_reason = str(failure_reason)
    # only reset these if the upload migration (PUT | MIGRATE) fails
    # if a GET fails then the migration is unaffected
    if upload_mig:
        mig_req.migration.stage = Migration.FAILED
        # Restore the file permissions so the users are not locked out of their
        # files! :)
        restore_owner_and_group(
            mig_req.migration,
            mig_req.migration.common_path
        )
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
        ).distinct().order_by('pk')
    # make sure we loop over all the archives in the (sub)set
    st_arch = 0#gr.last_archive
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


def get_ip_address():
    """Get an ip address using socket, or fake for testing purposes."""
    ip = socket.gethostbyname(socket.gethostname())
    # fake if running on VM
    if ip == '127.0.0.1':
        ip = '130.246.189.180'
    return ip
