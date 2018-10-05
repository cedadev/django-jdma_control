#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Return codes:
    0: OK
    1: config error
    2: log directory error
    3: already running
    4: error creating client
"""

import os
import sys
import multiprocessing
import signal
import random
import datetime
import getopt
import argparse
import subprocess
import logging

import elastic_tape.shared.error as err
from elastic_tape.shared.transport import localAddress
import elastic_tape.client
from jdma_control.scripts.common import setup_logging
from jdma_control.scripts.common import get_ip_address
from jdma_control.scripts.config import read_backend_config

processes = []

class TransferProcess(multiprocessing.Process):
    def setup(self):
        self.ET_Settings = read_backend_config("elastictape")
        self.host = self.ET_Settings["PUT_HOST"]
        self.port = self.ET_Settings["PORT"]
        self.shutdown = multiprocessing.Event()

    def run(self):
        try:
            while not self.shutdown.is_set():
                transfer = None
                try:
                    client = elastic_tape.client.connect(self.host, self.port)
                except err.StorageDError as e:
                    client.close()
                    logging.error(
                        'Caught error {} when trying to connect to ET'.format(e)
                    )
                    r = random.randint(5, 15)
                    self.shutdown.wait(r)
                else:
                    try:
                        try:
                            ip = get_ip_address()
                            transfer = client.getNextTransferrable(PI=ip)
                            if transfer is not None:
                                logging.info('Handling transfer {}'.format(
                                    (transfer.transferID)
                                ))
                                eList = transfer.verify()
                                if eList:
                                    for e in eList:
                                        logging.error(
                                            'Error {} in transfer ID {}'.format(
                                                e,transfer.transferID
                                            )
                                        )
                                        client.msgIface.sendError(e)
                                else:
                                    transfer.send()
                                    logging.info(
                                        'Done sending transfer {}'.format(
                                            transfer.transferID
                                        )
                                    )
                        except err.StorageDError as e:
                            if e.code == err.ECCHEFUL:
                                logging.error(
                                    'Server cache is full, waiting a while'
                                )
                                # Pause for a bit, to give the server a chance
                                # to free up some space
                                self.shutdown.wait(30)
                            else:
                                logging.error('Caught error {}'.format(e))
                    finally:
                        client.close()
                        if transfer is None:
                            r = random.randint(5, 15)
                            self.shutdown.wait(r)
        except Exception as e:
            logging.exception('Caught this: {}'.format(e))
        logging.debug('Process {} exiting'.format(self.name))

    def stop(self):
        logging.debug('Shutdown called for process: {}'.format(self.name))
        self.shutdown.set()

def shutdown_handler(signum, frame):
    global processes
    logging.debug('Shutdown called by signal: {}'.format(signum))
    for p in processes:
        p.stop()

def run(*args):

    setup_logging("jdma_control.backends.ElasticTapeTransport")

    shutdown = multiprocessing.Event()
    shutdown.clear()

    # First of all check if the process is running - if it is then don't start
    # running again
    n_procs = 0
    procs = subprocess.check_output(["ps", "-f", "-u", "root"]).decode("utf-8")
    for l in procs.split("\n"):
        if "ET_transfer_mp" in l and not "/bin/sh" in l:
            n_procs += 1

    if n_procs > 1:
        logging.error("Process already running, exiting")
        sys.exit(4)

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    # Limit the worker process count
    ET_Settings = read_backend_config("elastictape")
    transferNum = ET_Settings["THREADS"]
    for i in range(transferNum):
        p = TransferProcess()
        p.setup()
        processes.append(p)
        p.start()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.pause()

    for p in processes:
        p.stop()
    for p in processes:
        p.join()
        logging.debug('Process shutdown: {}'.format(p.name))

    sys.exit(0)

if __name__ == "__main__":
    run()
