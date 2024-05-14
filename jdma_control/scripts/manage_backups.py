# manage the database backups in the directory /data/pgsql/backups

from pathlib import Path
import os
import subprocess
from datetime import datetime

# time before backups get zipped
ZIP_TIME = 1

# maximum length of time persistant = 7 days
MAX_PERSISTANCE = 7

# backup directory
BACKUP_DIR = "/data/pgsql/backups"

def run(*args):    
    if "dry_run" in args:
        DRY_RUN = True
    else:
        DRY_RUN = False

    # list the backup directory
    backup_path = Path(BACKUP_DIR)

    # gzip
    for f in backup_path.glob("jdma_backup.*.json"):
        # check the mtime
        finfo = f.stat()
        now = datetime.now()
        file_time = datetime.fromtimestamp(finfo.st_mtime)
        days_old = (now - file_time).days
        if days_old >= ZIP_TIME:
            print("Gzipping backup file {} due to age {} days".format(f, days_old))
            if not DRY_RUN:
                subprocess.run(["gzip", f])

    # delete gzipped
    for f in backup_path.glob("jdma_backup.*.json.gz"):
        # check the mtime
        finfo = f.stat()
        now = datetime.now()
        file_time = datetime.fromtimestamp(finfo.st_mtime)
        days_old = (now - file_time).days
        if days_old >= MAX_PERSISTANCE:
            print("Deleting backup file {} due to age {} days".format(f, days_old))
            if not DRY_RUN:
                os.unlink(f)
