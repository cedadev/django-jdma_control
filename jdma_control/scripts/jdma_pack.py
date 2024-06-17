"""Functions to pack and unpack the files in a PUT | GET | MIGRATE request
   to / from a tarfile ready for transfer to the external storage (PUT) or
   once the transfer from the storage has completed (GET).
   Running this will change the state of the MigrationRequests
"""

import os
import logging
import signal, sys
from tarfile import TarFile
from time import sleep
import random
from multiprocessing import Process, Queue

from django.db.models import Q

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from jdma_control.models import StorageQuota
from jdma_control.scripts.jdma_transfer import mark_migration_failed
from jdma_control.scripts.jdma_transfer import get_download_dir
from jdma_control.scripts.common import get_archive_set_from_get_request
from jdma_control.scripts.common import split_args
import jdma_control.backends
from jdma_control.scripts.config import read_process_config
from jdma_control.scripts.config import get_logging_format, get_logging_level
from jdma_control.scripts.common import calculate_digest_adler32

def pack_archive(request_staging_dir, archive, pr):
    """Create a tar file containing the files that are in the
       MigrationArchive object"""

    # if the file exists then delete it!
    try:
        os.unlink(tar_file_path)
    except:
        pass
    # create the tar file
    tar_file = TarFile(tar_file_path, mode='w')
    logging.debug((
        "Created TarFile archive file: {}"
    ).format(tar_file_path))

    # get the MigrationFiles belonging to this archive
    migration_files = archive.migrationfile_set.all()
    # loop over the MigrationFiles in the MigrationArchive
    for mp in migration_paths:
        # don't add if it's a directory - files under the directory will
        # be added
        if not(os.path.isdir(mp[0])):
            tar_file.add(mp[0], arcname=mp[1])
            logging.debug((
                "    Adding file to TarFile archive: {}"
            ).format(mp[0]))

    tar_file.close()

    ### end of parallelisation

    return tar_file_path

def pack_archives(archive_list, q):
    """Pack the files in the archive_list into tar files"""
    for archive_info in archive_list:
        # first element is tarfile path / archive location
        tar_file_path = archive_info[0]
        try:
            os.unlink(tar_file_path)
        except:
            pass
        # create the tar file
        tar_file = TarFile(tar_file_path, mode='w')
        logging.debug((
            "Created TarFile archive file: {}"
        ).format(tar_file_path))

        # second element contains the MigrationFiles for this archive
        migration_paths = archive_info[1]
        # loop over the MigrationFiles in the MigrationArchive
        for mp in migration_paths:
            # don't add if it's a directory - files under the directory will
            # be added
            if not(os.path.isdir(mp[0])):
                tar_file.add(mp[0], arcname=mp[1])
                logging.debug((
                    "    Adding file to TarFile archive: {}"
                ).format(mp[0]))
        tar_file.close()
        # calculate digest (element 2), digest format (element 3)
        # and size (element 4) and add to archive
        archive_info[2] = calculate_digest_adler32(tar_file_path)
        archive_info[3] = "ADLER32"
        archive_info[4] = os.stat(tar_file_path).st_size

    q.put(archive_list)

def pack_request(pr, archive_staging_dir, config):
    """Pack a single request.  This has been split out so we can
    parallelise later"""
    # start at the last_archive so that interrupted packing can be resumed
    st_arch = 0
    ed_arch = pr.migration.migrationarchive_set.count()
    # get the archive set here as it might change if we get it in the loop
    archive_set = pr.migration.migrationarchive_set.order_by('pk')
    # get the subset of the archives that we are going to deal with
    active_archives = archive_set[st_arch:ed_arch]
    # build a list of the archives we should be packing
    # each element is a tuple of:
    # (archive location, [list of files in archive], archive_digest, archive_size)
    # each "file" in the list of files in archive is actually a tuple of
    # (location of file on file system, location of file in archive)
    # we can then pass that off to a multiprocess to do the actual packing
    # the archive digest and archive size will be filled in once the archive
    # has been packed
    archive_list = []

    # create the directory path for the batch
    request_staging_dir = os.path.join(
        archive_staging_dir,
        pr.migration.get_id()
    )

    # build the list of archives and files
    for archive in active_archives:
        if not archive.packed:
            archive.digest = "not packed"
            archive.save()
            continue

        if not (os.path.isdir(request_staging_dir)):
            os.makedirs(request_staging_dir)

        # create a path to store the tar file in
        tar_file_path = archive.get_archive_name(prefix=request_staging_dir)
        # get the MigrationFiles belonging to this archive
        migration_files = archive.get_file_names(
            prefix=pr.migration.common_path
        )['FILE']
        # get a list of files with a full file path
        migration_paths = []
        for mf in migration_files:
            # maintain a tuple of path on filesystem, path inside the tarfile
            migration_paths.append(
                (os.path.join(pr.migration.common_path, mf), mf)
            )
        # add to the archive_list so we can tar in parallel later
        archive_list.append(
            [tar_file_path,
             migration_paths,
             "", # digest
             "", # digest format
             0,  # size
             archive.pk, # id
            ]
        )
    # subdivide the archive list into the number of threads we have and then
    # dispatch each sublist of archives to its own thread
    if len(archive_list) > 0:
        n_threads = config["THREADS"]
        processes = []
        for tn in range(0, n_threads):
            local_archive_list = archive_list[tn::n_threads]
            q = Queue()
            p = Process(
                target = pack_archives,
                args = (local_archive_list, q)
            )
            p.start()
            processes.append((p, q))

        # block here until all threads have completed
        for p in processes:
            local_archive_list = p[1].get()
            # assign the digests and sizes to the archive
            for la in local_archive_list:
                archive = pr.migration.migrationarchive_set.get(pk=la[5])
                archive.digest = la[2]
                archive.digest_format = la[3]
                archive.size = la[4]
                archive.save()
            p[0].join()

    # the request has completed so transition the request to PUTTING and reset
    # the last archive
    pr.stage = MigrationRequest.PUT_PENDING
    logging.info((
        "Transition: request ID: {} PUT_PACKING->PUT_PENDING"
    ).format(pr.pk))
    pr.last_archive = 0
    pr.save()


def put_packing(backend_object, config):
    """Pack the ArchiveFiles into a TarFile in the ARCHIVE_STAGING_DIR
    for this backend"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the PUT requests for this backend which are in the PACKING stage
    pr = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(locked=False)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.PUT_PACKING)
    ).first()
    if not pr:
        return
    try:
        if not pr.lock():
            return
        pack_request(
            pr,
            backend_object.ARCHIVE_STAGING_DIR,
            config
        )
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
    archive_path = archive.get_archive_name(archive_staging_dir)
    # create the target directory if it doesn't exist
    try:
        os.makedirs(target_path)
    except:
        pass

    try:
        tar_file = TarFile(archive_path, 'r')
        # check that the tar_file digest matches the digest in the database
        digest = calculate_digest(archive_path)
        if digest != archive.digest:
            error_string = (
                "Digest does not match for archive: {}"
            ).format(archive_path)
            raise Exception(error_string)
    except:
        error_string = (
            "Could not find archive path: {}"
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
            logging.debug((
                "    Extracting file: {} from archive: {} to directory: {}"
            ).format(tar_info.name, archive.get_id(), target_path))
        except Exception as e:
            error_string = (
                "Could not extract file: {} from archive {} to path: {}, exception: {}"
            ).format(tar_info.name, archive.get_id(), target_path, str(e))
            logging.error(error_string)
            raise Exception(error_string)

    tar_file.close()

def unpack_archives(archive_list):
    # Each element of the archive list is a tuple:
    # [0] = archive_staging_dir
    # [1] = archive
    # [2] = external_id
    # [3] = target_path
    # [4] = filelist
    for archive in archive_list:
        unpack_archive(archive[0],
                       archive[1],
                       archive[2],
                       archive[3],
                       archive[4]
                      )

def unpack_request(gr, archive_staging_dir, config):
    """Unpack a single request.  This has been split out so we can
    parallelise later"""

    archive_set, st_arch, n_arch = get_archive_set_from_get_request(gr)

    # loop over the archives to create a list of archives to unpack
    archive_list = []
    for arch_num in range(st_arch, n_arch):
        # determine which archive to unpack
        archive = archive_set[arch_num]
        if archive.packed:
            archive_list.append((archive_staging_dir,
                                 archive,
                                 gr.migration.external_id,
                                 gr.target_path,
                                 gr.filelist)
                               )

    # subdivide the archive list into the number of threads we have and then
    # dispatch each sublist of archives to its own thread
    processes = []
    if len(archive_list) > 0:
        n_threads = config["THREADS"]
        processes = []
        for tn in range(0, n_threads):
            local_archive_list = archive_list[tn::n_threads]
            p = Process(
                target = unpack_archives,
                args = (local_archive_list, )
            )
            p.start()
            processes.append(p)

        # block here until all threads have completed
        for p in processes:
            p.join()

    # the request has completed so transition the request to GET_RESTORE and
    # reset the last archive
    gr.stage = MigrationRequest.GET_RESTORE
    logging.info((
        "Transition: request ID: {} GET_UNPACK->GET_RESTORE"
    ).format(gr.pk))
    gr.last_archive = 0
    gr.save()


def get_unpacking(backend_object, config):
    """Unpack the ArchiveFiles from a TarFile to a target directory"""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())
    # Get the GET requests for this backend which are in the PACKING stage.
    gr = MigrationRequest.objects.filter(
        Q(request_type=MigrationRequest.GET)
        & Q(locked=False)
        & Q(migration__storage__storage=storage_id)
        & Q(stage=MigrationRequest.GET_UNPACKING)
    ).first()
    if not gr:
        return

    try:
        if not gr.lock():
            return
        unpack_request(
            gr,
            get_download_dir(backend_object, gr),
            config
        )
        gr.unlock()
    except Exception as e:
        error_string = (
            "Could not unpack request for batch: {}: {}"
        ).format(str(gr.migration.external_id), str(e))
        logging.error(error_string)
        mark_migration_failed(gr, error_string, e, upload_mig=True)


def process(backend, config):
    backend_object = backend()
    put_packing(backend_object, config)
    get_unpacking(backend_object, config)


def exit_handler(signal, frame):
    logging.info("Stopping jdma_pack")
    sys.exit(0)


def run_loop(backend, config):
    # moved this to a function so we can call a one-shot version
    if backend is None:
        for backend in jdma_control.backends.get_backends():
            process(backend, config)
    else:
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            process(backend, config)


def run(*args):
    """Entry point for the Django script run via ``./manage.py runscript``
    optionally pass the backend_id in as an argument
    """
    config = read_process_config("jdma_pack")
    logging.basicConfig(
        format=get_logging_format(),
        level=get_logging_level(config["LOG_LEVEL"]),
        datefmt='%Y-%d-%m %I:%M:%S'
    )
    logging.info("Starting jdma_pack")

    # setup exit signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # process the arguments
    arg_dict = split_args(args)
    if "backend" in arg_dict:
        backend = arg_dict["backend"]
    else:
        backend = None

    # decide whether to run as a daemon
    if "daemon" in arg_dict:
        if arg_dict["daemon"].lower() == "true":
            daemon = True
        else:
            daemon = False
    else:
        daemon = False

    # run as a daemon or one shot
    if daemon:
        # loop this indefinitely until the exit signals are triggered
        while True:
            run_loop(backend, config)
            sleep(5 + random.random())
    else:
        run_loop(backend, config)
