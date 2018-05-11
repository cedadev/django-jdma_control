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
from jdma_control.scripts.jdma_transfer import mark_migration_failed
from jdma_control.scripts.jdma_transfer import get_download_dir
from jdma_control.scripts.common import get_archive_set_from_get_request
import jdma_control.backends


def pack_archive(archive_staging_dir, archive, pr):
    """Create a tar file containing the files that are in the
       MigrationArchive object"""
    # internal id and common_path
    internal_id = pr.migration.get_id()
    common_path = pr.migration.common_path

    # create the directory path for the batch (internal_id)
    archive_path = os.path.join(
        archive_staging_dir,
        internal_id
    )
    if not (os.path.isdir(archive_path)):
        os.makedirs(archive_path)

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
        # don't add if it's a directory - files under the directory will
        # be added
        # get the path, it's the common_path + the migration file path
        path = os.path.join(common_path, mf.path)
        if not(os.path.isdir(path)):
            tar_file.add(path, arcname=mf.path)
            logging.info((
                "    Adding file to TarFile archive: {}"
            ).format(path))

    tar_file.close()
    # set the size of the archive
    archive.size = os.stat(tar_file_path).st_size
    archive.save()
    return tar_file_path


def pack_request(pr, archive_staging_dir):
    """Pack a single request.  This has been split out so we can
    parallelise later"""
    # start at the last_archive so that interrupted packing can be resumed
    st_arch = pr.last_archive
    n_arch = pr.migration.migrationarchive_set.count()
    # get the archive set here as it might change if we get it in the loop
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        # stage the archive - i.e. create the tar file
        archive_path = pack_archive(
            archive_staging_dir,
            archive,
            pr
        )
        # calculate digest and add to archive
        archive.digest = calculate_digest(archive_path)
        archive.save()
        # update the last good archive
        pr.last_archive += 1
        pr.save()
    # the request has completed so transition the request to PUTTING and reset
    # the last archive
    pr.stage = MigrationRequest.PUT_PENDING
    pr.last_archive = 0
    pr.save()


def put_packing(backend_object):
    """Pack the ArchiveFiles into a TarFile in the ARCHIVE_STAGING_DIR
    for this backend"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend which are in the PACKING stage
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_PACKING)
    )
    # for each PUT request pack the archive files.
    # split into a loop so it can be parallelised later
    for pr in put_reqs:
        # check if locked
        if pr.locked:
            continue
        try:
            pr.lock()
            pack_request(pr, backend_object.ARCHIVE_STAGING_DIR)
            pr.unlock()
        except Exception as e:
            error_string = (
                "Could not pack archive for batch: {}: {}"
            ).format(pr.migration.get_id(), str(e))
            # mark the migration as failed
            mark_migration_failed(pr, error_string, e)


def unpack_archive(archive_staging_dir, archive, external_id,
                   target_path, filelist=None):
    """Unpack a tar file containing the files that are in the
       MigrationArchive object"""
    # create the name of the archive
    archive_path = os.path.join(
        archive_staging_dir,
        archive.get_id()) + ".tar"
    # create the target directory if it doesn't exist
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    # see if the archive exists
    if not (os.path.exists(archive_path)):
        error_string = (
            "Could not find archive path: {}"
        ).format(archive_path)
        raise Exception(error_string)
    tar_file = TarFile(archive_path, 'r')
    # check that the tar_file digest matches the digest in the database
    digest = calculate_digest(archive_path)
    if digest != archive.digest:
        error_string = (
            "Digest does not match for archive: {}"
        ).format(archive_path)
        raise Exception(error_string)

    # untar each file
    for tar_info in tar_file.getmembers():
        try:
            # if filelist only extract those in the filelist
            if filelist:
                if tar_info.name in filelist:
                    tar_file.extract(tar_info, path=target_path)
            else:
                tar_file.extract(tar_info, path=target_path)
            logging.info((
                "    Extracting file: {} from archive: {} to directory: {}"
            ).format(tar_info.name, archive.get_id(), target_path))
        except Exception as e:
            error_string = (
                "Could not extract file: {} from archive {} to path: {}, exception: {}"
            ).format(tar_info.name, archive.get_id(), target_path, str(e))
            logging.error(error_string)
            raise Exception(error_string)

    tar_file.close()


def unpack_request(gr, archive_staging_dir):
    """Unpack a single request.  This has been split out so we can
    parallelise later"""

    archive_set, st_arch, n_arch = get_archive_set_from_get_request(gr)

    # loop over the archives
    for arch_num in range(st_arch, n_arch):
        # determine which archive to stage (tar) and upload
        archive = archive_set[arch_num]
        unpack_archive(archive_staging_dir, archive,
                       gr.migration.external_id,
                       gr.target_path,
                       gr.filelist)
        # update the last good archive
        gr.last_archive += 1
        gr.save()
    # the request has completed so transition the request to PUTTING and reset
    # the last archive
    gr.stage = MigrationRequest.GET_RESTORE
    gr.last_archive = 0
    gr.save()


def get_unpacking(backend_object):
    """Unpack the ArchiveFiles from a TarFile to a target directory"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the GET requests for this backend which are in the PACKING stage.
    get_reqs = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.GET_UNPACKING)
    )
    # loop over the GET requests and unpack each archive
    # split into a loop so it can be parallelised later
    for gr in get_reqs:
        try:
            # check locked
            if gr.locked:
                continue
            gr.lock()
            unpack_request(gr, get_download_dir(backend_object, gr))
            gr.unlock()
        except Exception as e:
            error_string = (
                "Could not unpack request for batch: {}: {}"
            ).format(str(gr.migration.external_id), str(e))
            logging.error(error_string)
            mark_migration_failed(gr, error_string, e, upload_mig=True)


def process(backend):
    backend_object = backend()
    put_packing(backend_object)
    get_unpacking(backend_object)


def run(*args):
    # setup the logging
    setup_logging(__name__)
    if len(args) == 0:
        for backend in jdma_control.backends.get_backends():
            process(backend)
    else:
        backend = args[0]
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend)
