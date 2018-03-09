"""Create the object store groupworkspace names, gws manager, quota and email
   from the elastic tape quotas and create user records, groupworkspace records
   and object store quotas.
   This script is run via ./manage runscript"""

import jdma_site.settings as settings
from jdma_control.models import User, Groupworkspace, StorageQuota
from jdma_control.scripts.jdma_lock import setup_logging
import requests

ET_EXPORT_URL = "http://cedadb.ceda.ac.uk/gws/etexport/"

def get_et_gws_from_url(url):
    # Fetch the (plain text) file of the gws and et quotas
    # the format is:
    # gws_name, user_name of manager, quota in bytes, email address of manager
    response = requests.get(url)
    if response.status_code == 200:
        data = response.content.decode('utf-8')
        lines = data.split("\n")
        ret_data = []
        for l in lines:
            d = l.strip().split(",")
            ret_data.append(d)
        return ret_data
    else:
        #logging.error("Could not read from URL: " + url)
        print("Could not read from URL: " + url)
        return None


def create_user_gws_quotas(data):
    # Create the User, GroupWorkspace and StorageQuota from each line of the
    # data
    for line in data:
        if len(line) == 4:
            # create a user (if it doesn't exist) and fill the data
            new_user = User.objects.filter(name=line[1])
            user_found = len(new_user) != 0
            if not user_found:
                new_user = User()
                new_user.name = line[1]
                new_user.email = line[3]
                new_user.save()
            else:
                new_user = new_user[0]

            # create a gws and fill the data (if not exist)
            new_gws = Groupworkspace.objects.filter(workspace=line[0])
            gws_found = len(new_gws)
            if not gws_found:
                new_gws = Groupworkspace()
                new_gws.save()
                new_gws.workspace = line[0]
                new_gws.managers.add(new_user)
                new_gws.save()
            else:
                new_gws = new_gws[0]

            # create the new storage quota
            storageid = StorageQuota.get_storage_index("objectstore")
            new_sq = StorageQuota.objects.filter(
                workspace=new_gws
            ).filter(storage=storageid)
            sq_found = len(new_sq) != 0
            if not sq_found:
                new_sq = StorageQuota()
                new_sq.storage = storageid
                # default to 32TB
                new_sq.quota_size = 32 * 10**12
                # no quota used yet for objectstore
                new_sq.quota_used = 0
                new_sq.workspace = new_gws
                new_sq.save()
            else:
                new_sq = new_sq[0]
                # update quotas
                new_sq.quota_size = 32 * 10**12
                new_sq.quota_used = 0
                new_sq.save()


def run():
    # setup_logging(__name__)
    data = get_et_gws_from_url(ET_EXPORT_URL)
    create_user_gws_quotas(data)
