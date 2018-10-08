"""Create the object store groupworkspace names, gws manager, quota and email
   from the elastic tape quotas and create user records, groupworkspace records
   and object store quotas.
   This script is run via ./manage runscript"""

import jdma_site.settings as settings
from jdma_control.models import User, Groupworkspace, StorageQuota
from jdma_control.scripts.import_et_gws import get_et_gws_from_url
from jdma_control.scripts.import_et_gws import create_user_entry, create_quota_entry

ET_EXPORT_URL = "http://cedadb.ceda.ac.uk/gws/etexport/"

def create_user_gws_quotas(data):
    # Create the User, GroupWorkspace and StorageQuota from each line of the
    # data
    storageid = StorageQuota.get_storage_index("objectstore")
    for line in data:
        if len(line) == 4:
            # create user entry
            new_gws = create_user_entry(line)
            # create the new storage quota
            create_quota_entry(storageid, new_gws, 32 * 10**12, 0)

def run():
    data = get_et_gws_from_url(ET_EXPORT_URL)
    create_user_gws_quotas(data)
