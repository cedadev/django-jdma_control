"""Functions to pack and unpack the files in a PUT | GET | MIGRATE request
   to / from a tarfile ready for transfer to the external storage (PUT) or
   once the transfer from the storage has completed (GET).
   Running this will change the state of the MigrationRequests
"""

import os
import logging
from tarfile import TarFile

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.models import StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging, calculate_digest
import jdma_control.backends


def pack_archive(archive_staging_dir, archive, external_id):
    """Create a tar file containing the files that are in the
       MigrationArchive object"""
    # create the directory path for the batch (external_id)
    archive_path = os.path.join(
        archive_staging_dir,
        external_id
    )
    if not (os.path.isdir(archive_path)):
        try:
            os.makedirs(archive_path)
        except Exception:
            error_string = (
                "Could not created archive path: {}"
            ).format(archive_path)
            logging.error(error_string)
            raise Exception(error_string)

    # create the tar file path
    tar_file_path = os.path.join(archive_path, archive.get_id()) + ".tar"
    # if the file exists then delete it!
    if os.path.exists(tar_file_path):
        os.unlink(tar_file_path)
    # create the tar file
    tar_file = TarFile(tar_file_path, mode='w')
    logging.info((
        "Created TarFile archive file: {}"
    ).format(tar_file_path))

    # get the MigrationFiles belonging to this archive
    migration_files = archive.migrationfile_set.all()
    # loop over the MigrationFiles in the MigrationArchive
    for mf in migration_files:
        try:
            # don't add if it's a directory - files under the directory will
            # be added
            if not(os.path.isdir(mf.path)):
                tar_file.add(mf.path)
                logging.info((
                    "    Adding file to TarFile archive: {}"
                ).format(mf.path))

        except Exception as e:
            error_string = (
                "Could not add file: {} to TarFile archive: {}"
            ).format(mf.path, archive_path)
            logging.error(error_string)
            raise Exception(e)
    tar_file.close()
    # set the size of the archive
    archive.size = os.stat(tar_file_path).st_size
    archive.save()
    return tar_file_path


def pack_request(pr, archive_staging_dir):
    """Pack a single request.  Split this out so we can parallelise later"""
    # start at the last_archive so that interrupted packing can be resumed
    st_arch = pr.last_archive
    n_arch = pr.migration.migrationarchive_set.count()
    # get the archive set here as it might change if we get it in the loop
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # stage the archive - i.e. create the tar file
        try:
            archive_path = pack_archive(
                archive_staging_dir,
                archive,
                pr.migration.external_id
            )
            # calculate digest and add to archive
            archive.digest = calculate_digest(archive_path)
            archive.save()
        except Exception as e:
            raise Exception(e)
        # update the last good archive
        pr.last_archive += 1
        pr.save()
    # the request has completed so transition the request to PUTTING and reset
    # the last archive
    pr.stage = MigrationRequest.PUTTING
    pr.last_archive = 0
    pr.save()


def put_packing(backend_object):
    """Pack the ArchiveFiles into a TarFile in the ARCHIVE_STAGING_DIR
    for this backend"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend.
    # This involves resolving two foreign keys
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_PACKING
        )
    )
    # for each PUT request pack the archive files.
    # split into a loop so it can be parallelised later
    for pr in put_reqs:
        pack_request(pr, backend_object.ARCHIVE_STAGING_DIR)


def get_unpacking(backend_object):
    pass


def run():
    # setup the logging
    setup_logging(__name__)
    # loop over all backends - should we parallelise these?
    for backend in jdma_control.backends.get_backends():
        backend_object = backend()
        put_packing(backend_object)
        get_unpacking(backend_object)
