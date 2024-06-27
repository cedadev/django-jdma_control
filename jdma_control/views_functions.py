"""Support functions for the views.py"""

from django.http import HttpResponse
import os
import json

from collections import namedtuple
import subprocess

from jasmin_ldap.core import *
from jasmin_ldap.query import *

import jdma_site.settings as settings


def HttpError(error_data, status=404):
    return HttpResponse(
        json.dumps(error_data),
        content_type="application/json",
        status=status,
        reason=error_data["error"]
    )


def python_ls(path):
    """Perform and interpret an ls on the path"""

    # create a nice way of returning the info
    ls_result = namedtuple('ls_result', ['mode', 'uid', 'gid'])

    # run the ls, do not descend into directories and list uid and gid as numbers
    proc_res = subprocess.run(["sudo", "ls", "-dn", path], stdout=subprocess.PIPE)

    # check it returned
    if proc_res.returncode != 0:
        raise Exception("File or directory not found: {}".format(path))

    # split the return into the permissions, uid and gid
    split_res = proc_res.stdout.decode("utf-8").split(" ")

    # return the mode as u perms, g perms, o perms
    ls_result.mode = (split_res[0][1:4], split_res[0][4:7], split_res[0][7:10])
    ls_result.uid = int(split_res[2])
    ls_result.gid = int(split_res[3])

    return ls_result


def user_has_write_permission(path, user):
    """Determine whether a particular user has write permission to a
    directory / file at path.
    The rules used to determine are:
      1. If "all" have write permission -> True
      2. If "group" has write permission and the user is a member of the group -> True
      3. If "user" has write permission and the user is the owner -> True
      4. -> False
    """
    # create the LDAP servers
    ldap_servers = ServerPool(
        settings.JDMA_LDAP_PRIMARY,
        settings.JDMA_LDAP_REPLICAS
    )

    # get the file status, using the system functions to allow apached to run ls as sudo
    ls_res = python_ls(path)

    # check for all
    if "wx" in ls_res.mode[2] or "ws" in ls_res.mode[2]:
        return True

    # check for group
    with Connection.create(ldap_servers) as conn:
        if "wx" in ls_res.mode[1] or "ws" in ls_res.mode[1]:
            # now we need to check that user is part of the group that owns
            # the file at path
            group = ls_res.gid
            query = Query(
                conn,
                base_dn=settings.JDMA_LDAP_BASE_GROUP
            ).filter(gidNumber=group)

            # check for a valid return
            if len(query) == 0:
                logging.error((
                    "Group with gidNumber: {} not found from LDAP"
                ).format(group))
                return False

            if query.count() != 0 and user in query[0]['memberUid']:
                return True
        # check for user
        if "wx" in ls_res.mode[0] or "ws" in ls_res.mode[0]:
            # check that the owner of the file matches the user
            # get the uid of the user
            query = Query(
                conn,
                base_dn=settings.JDMA_LDAP_BASE_USER
            ).filter(uid=user)
            if len(query) == 0:
                logging.error((
                    "Unix user id: {} not found from LDAP in "
                    "UserHasWritePermission"
                ).format(user))
                return False
            if ls_res.uid == query[0]["uidNumber"][0]:
                return True
    return False


def user_has_sufficient_diskspace(path, user, size):
    return True
