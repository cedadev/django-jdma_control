"""Functions to transition a migration request from:
   ON_DISK -> PUT_PENDING     - locks the directory for migration by changing the owner to root / jdma user
   ON_STORAGE -> GET_PENDING     - create the target directory, and lock it by changing the owner again

   This is a simple program that is designed to be run at high-frequency, e.g. every minute even.
"""

import datetime
import logging
import subprocess
import os

import jdma_site.settings as settings
from jdma_control.models import Migration, MigrationRequest
from django.db.models import Q


def setup_logging(module_name):
    # setup the logging
    try:
        log_path = settings.LOG_PATH
    except:
        log_path = "./"

    # Make the logging dir if it doesn't exist
    if not os.path.isdir(log_path):
        os.makedirs(log_path)

    date = datetime.datetime.utcnow()
    date_string = "%d%02i%02iT%02i%02i%02i" % (date.year, date.month, date.day, date.hour, date.minute, date.second)
    log_fname = log_path + "/" + module_name+ "_" + date_string

    logging.basicConfig(filename=log_fname, level=logging.DEBUG)


def lock_put_directories():
    """Lock the directories that are going to be put to external storage.
       This is to ensure that the user doesn't write any more data to them while the external storage write is ongoing.
    """
    # get the list of PUT requests
    put_reqs = MigrationRequest.objects.filter(Q(request_type=MigrationRequest.PUT) | Q(request_type=MigrationRequest.MIGRATE))
    # for each PUT request get the Migration and determine if the type of the Migration is ON_DISK
    for pr in put_reqs:
        if pr.migration.stage == Migration.ON_DISK:
            # if it's on disk then:
            # 1. change the owner of the directory to be root
            # 2. change the read / write permissions to be user-only
            subprocess.call(["/usr/bin/sudo", "/bin/chown", "root:root", pr.migration.original_path])
            subprocess.call(["/usr/bin/sudo", "/bin/chmod", "700", pr.migration.original_path])
            # set the migration stage to be PUT_PENDING
            pr.migration.stage = Migration.PUT_PENDING
            pr.migration.save()
            logging.info("PUT: Locked directory: " + pr.migration.original_path)


def lock_get_directories():
    """Lock the directories that the targets for recovering data from external storage.
       This is to ensure that there aren't any filename conflicts."""
    # get the list of GET requests
    get_reqs = MigrationRequest.objects.filter(request_type=MigrationRequest.GET)
    # for each GET request get the Migration and determine if the type of the Migration is ON_TAP
    for gr in get_reqs:
        if gr.stage == MigrationRequest.ON_STORAGE and gr.migration.stage == Migration.ON_STORAGE:
            # if it's on external storage then:
            # 1. Make the directory if it doesn't exist
            # 2. change the owner of the directory to be root
            # 3. change the read / write permissions to be user-only
            if not os.path.isdir(gr.target_path):
                subprocess.call(["/usr/bin/sudo", "/bin/mkdir", gr.target_path])
            subprocess.call(["/usr/bin/sudo", "/bin/chown", "root:root", gr.target_path])
            subprocess.call(["/usr/bin/sudo", "/bin/chmod", "700", gr.target_path])
            # set the migration stage to be GET_PENDING
            gr.stage = MigrationRequest.GET_PENDING
            gr.save()
            logging.info("GET: Locked directory: " + gr.target_path)
            logging.info("Transition: request ID: {} ON_STORAGE->GET_PENDING".format(gr.pk))


def run():
    """Entry point for the Django script run via ``./manage.py runscript``
    """
    setup_logging(__name__)
    lock_put_directories()
    lock_get_directories()
