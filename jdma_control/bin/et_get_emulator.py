#! /usr/bin/env python
# Emulated version of et_get
# Only the command line arguments required for the JDMA are emulated

import os
import getopt
import sys

from jdma_site.settings import FAKE_ET_DIR
from shutil import copy

def Usage():
    print("Usage:")
    print("    et_get.py [ -v ] [ -t threads ] [ -l logfile ] [ -h hostname ] [ -p port ] [ -w workspace ] { -d dataset | -f sourcefile | -b batchID } [ -r restoredirectory ]")
    sys.exit(1)


def get_from_fake_et(batch_id, target_dir):
    # download a batch from the fake_et
    # get the directory which has the batch in it
    batch_dir = os.path.join(FAKE_ET_DIR, "batch%04i" % batch_id)
    # walk the directory and copy
    user_file_list = os.walk(batch_dir, followlinks=False)
    # copy full paths into a list
    filepaths = []
    for root, dirs, files in user_file_list:
        if len(files) != 0:
            for fl in files:
                # get the full path and append to the list
                filepath = os.path.join(root, fl)
                filepaths.append(filepath)
    # copy the files to the verify directory
    for fp in filepaths:
        # strip the batch_dir from the fp
        target_fp = target_dir + fp.replace(batch_dir, "")
        # get just the path
        target_path = os.path.dirname(target_fp)
        if not os.path.isdir(target_path):
            os.makedirs(target_path)
        copy(fp, target_fp)


if __name__ == "__main__":
    batch_id = -1
    logfile = ''
    target_dir = ''
    workspace = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'b:r:l:w:')
    except getopt.GetoptError as err:
        print(str(err))
        Usage()

    for o, a in opts:
        if o == "-w":
            workspace = a
        elif o == "-r":
            target_dir = a
            if not os.access(target_dir, os.W_OK):
                print("No permission to write to target directory")
                Usage()
        elif o == "-b":
            batch_id = int(a)
        else:
            Usage()

    # analog for download_batch
    get_from_fake_et(batch_id, target_dir)
