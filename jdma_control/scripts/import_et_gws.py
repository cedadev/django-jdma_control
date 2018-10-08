"""Import the elastic tape groupworkspace names, gws manager, quota and email
   and create user records, groupworkspace records and elastic tape quotas.
   This script is run via ./manage runscript"""

import jdma_site.settings as settings
from jdma_control.models import User, Groupworkspace, StorageQuota
from xml.dom.minidom import parseString
import requests

ET_QUOTA_URL = "http://et-monitor.fds.rl.ac.uk/et_user/ET_Holdings_Summary_XML.php"
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

def create_user_entry(line):
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

    return new_gws

def create_quota_entry(storageid, new_gws, quota_size, quota_used):
    new_sq = StorageQuota.objects.filter(
        workspace=new_gws
    ).filter(storage=storageid)
    sq_found = len(new_sq) != 0
    if not sq_found:
        new_sq = StorageQuota()
        new_sq.storage = storageid
        new_sq.quota_size = quota_size
        new_sq.quota_used = quota_used
        new_sq.workspace = new_gws
        new_sq.save()
    else:
        new_sq = new_sq[0]
        # update quota if necessary
        new_sq.quota_size = quota_size
        new_sq.quota_used = quota_used
        new_sq.save()


def create_user_gws_quotas(data):
    # Create the User, GroupWorkspace and StorageQuota from each line of the
    # data
    storageid = StorageQuota.get_storage_index("elastictape")
    for line in data:
        if len(line) == 4:
            # create the user entry using the above script
            new_gws = create_user_entry(line)
            # get the quota and quota used
            quota, quota_used = get_et_quota_used(ET_QUOTA_URL, line[0])
            # create the new storage quota and assign the workspace
            create_quota_entry(storageid, new_gws, int(line[2]), quota_used)


def get_et_quota_used(url, workspace):
    # This requires interpreting a webpage at url
    # build the url to the table
    quota_url = url + "?workspace=" + workspace + "&caller=etjasmin"
    quota_xml = requests.get(quota_url)
    if quota_xml.status_code != 200:
        raise Exception("Could not read quota URL: " + quota_url)
    # try parsing the XML into a XML dom
    try:
        # get the quota amount and the quota used
        quota_dom = parseString(quota_xml.content)
        quota = int(quota_dom.getElementsByTagName("quota")[0].firstChild.data)
        quota_used = int(quota_dom.getElementsByTagName(
                         "quota_used")[0].firstChild.data)
        quota_dom.unlink()
        # calculate remaining quota
        quota_remaining = quota - quota_used
    except Exception:
        raise Exception("Could not parse XML document at: " + quota_url)
    return quota, quota_used


def run():
    data = get_et_gws_from_url(ET_EXPORT_URL)
    create_user_gws_quotas(data)
