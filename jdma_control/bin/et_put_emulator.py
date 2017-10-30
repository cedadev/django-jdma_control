#! /usr/bin/env python
# Emulated version of et_put
# Only the command line arguments required for the JDMA are emulated

import os
import getopt
import sys
import random
from shutil import copy2
import time

from jdma_site.settings import FAKE_ET_DIR

def Usage():
    print("Usage:")
    print("    et_put.py [ -v ] [ -c ] [ -l logfile ] [ -h hostname ] [ -p port ] [ -w workspace ] [ -t tag ] [-t one-word-tag] { -f sourcefilelist | source... }")
    sys.exit(1)


def put_to_fake_et(sourcefilelist):
    # open the file
    fh = open(sourcefilelist, 'r')
    lines = fh.readlines()
    # don't do anything if no files in the list
    if len(lines) == 0:
        return -1
    fh.close()
    # create a batchid as a random number between 0 and 9999
    batch_id = random.uniform(0,9999)
    # create the directory to store the batch in
    batch_dir = os.path.join(FAKE_ET_DIR, "batch%04i" % batch_id)
    if not os.path.isdir(batch_dir):
        os.makedirs(batch_dir)
    # now copy the files
    for f in lines:
        # get the source filename
        fname = f.strip()
        if fname[0] == "/":
            dest_fname = fname[1:]
        else:
            dest_fname = fname
        # create the destination filename
        dest_file = os.path.join(batch_dir, dest_fname)
        dest_dir = os.path.dirname(dest_file)
        # create the destination directory if it doesn't exist
        if not os.path.isdir(dest_dir):
            os.makedirs(dest_dir)
        # do the copy
        copy2(fname, dest_file)
    return batch_id


if __name__ == "__main__":
    sourcefilelist = None
    logfile = ''
    workspace = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'w:l:f:')
    except getopt.GetoptError as err:
        print(str(err))
        Usage()

    for o, a in opts:
        if o == "-w":
            workspace = a
        elif o == "-f":
            sourcefilelist = a

    if sourcefilelist is not None:
        batch_id = put_to_fake_et(sourcefilelist)
        if batch_id == -1:
            print("BATCH REJECTED")
        else:
            print("Batch ID: %i" % batch_id)
    else:
        Usage()
