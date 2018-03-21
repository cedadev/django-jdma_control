"""Functions to verify files that have been migrated to external storage.
   These files have been put on external storage and then (temporarily) pulled back to
   disk before being verified by calculating the SHA256 digest and comparing it
   to the digest that was calculated (in jdma_transfer) before it was uploaded
   to external storage.
   Running this will change the state of the migrations:
     VERIFYING->ON_STORAGE
"""

import os
import sys
import logging

from django.db.models import Q

from jdma_control.models import Migration, MigrationRequest, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging, calculate_digest
from jdma_control.scripts.jdma_transfer import mark_migration_failed
import jdma_control.backends

def get_permissions_string(p):
    # this is unix permissions
    is_dir = 'd'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = oct(p)[-3:]
    return is_dir + ''.join(dic.get(x,x) for x in perm)


def verify_files(backend_object):
    """Verify the files that have been uploaded to external storage and then
    downloaded back to a temporary directory."""
    # get the storage id for the backend object
    storage_id = StorageQuota.get_storage_index(backend_object.get_id())

    # these are part of a PUT request - get the list of PUT request
    put_reqs = MigrationRequest.objects.filter(
        (Q(request_type=MigrationRequest.PUT)
        | Q(request_type=MigrationRequest.MIGRATE))
        & Q(stage=MigrationRequest.VERIFYING)
        & Q(migration__storage__storage=storage_id)
    )
    for pr in put_reqs:
        # check whether locked
        if pr.locked:
            continue
        # lock the migration
        pr.lock()
        # get the batch id
        external_id = pr.migration.external_id
        # get the temporary directory
        verify_dir = os.path.join(
            backend_object.VERIFY_DIR,
            "verify_{}".format(pr.migration.external_id)
        )
        # loop over the MigrationArchives that belong to this Migration
        archive_set = pr.migration.migrationarchive_set.order_by('pk')
        # use last_archive to enable restart of verification
        st_arch = pr.last_archive
        n_arch = archive_set.count()
        for arch_num in range(st_arch, n_arch):
            # determine which archive to stage (tar) and upload
            archive = archive_set[arch_num]
            # filename is concatenation of verify_dir and the the original
            # file path
            verify_file_path = os.path.join(
                verify_dir,
                archive.get_id()
            ) + ".tar"
            # check the file exists - if it doesn't then set the stage to
            # FAILED and write that the file couldn't be found in the
            # failure_reason
            if not os.path.exists(verify_file_path):
                failure_reason = (
                    "VERIFY: archive {} could not be found."
                ).format(verify_file_path)
                mark_migration_failed(pr, failure_reason)
                break
            else:
                # check that the digest matches
                new_digest = calculate_digest(verify_file_path)
                # check that the digests match
                if new_digest != archive.digest:
                    failure_reason = (
                        "VERIFY: archive {} has a different digest."
                    ).format(verify_file_path)
                    mark_migration_failed(pr, failure_reason)
                    break
            # add one to last archive
            pr.last_archive += 1
            pr.save()
        # if we reach this part without exiting then the batch has verified
        # successfully and we can transition to PUT_TIDY, ready for the
        # tidy up process
        pr.stage = MigrationRequest.PUT_TIDY
        # reset last archive
        pr.last_archive = 0
        # unlock
        pr.locked = False
        pr.save()
        logging.info((
            "Transition: batch ID: {} VERIFYING->PUT_TIDY"
        ).format(pr.migration.external_id))


def run(*args):
    # setup the logging
    setup_logging(__name__)
    if len(args) == 0:
        for backend in jdma_control.backends.get_backends():
            backend_object = backend()
            verify_files(backend_object)
    else:
        backend = args[0]
        if not backend in jdma_control.backends.get_backend_ids():
            logging.error("Backend: " + backend + " not recognised.")
        else:
            backend = jdma_control.backends.get_backend_from_id(backend)
            backend_object = backend()
            verify_files(backend_object)
