"""Support functions for the views.py"""

from django.http import HttpResponse
import os
import json

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
    ldap_servers = ServerPool(settings.JDMA_LDAP_PRIMARY, settings.JDMA_LDAP_REPLICAS)

    # get the file status, covert to octal and mask to the lower 3 octal values
    fstat = os.stat(path)
    mode = fstat.st_mode & 0o777
    # check for all
    if mode & 0o002:
        return True
    # check for group
    with Connection.create(ldap_servers) as conn:
        if mode & 0o020:
            # now we need to check that user is part of the group that owns
            # the file at path
            group = fstat.st_gid
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

            if len(query) != 0 and user in query[0]['memberUid']:
                return True
        # check for user
        if mode & 0o200:
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

            if fstat.st_uid == query[0]["uidNumber"][0]:
                return True
    return False


def user_has_sufficient_diskspace(path, user, size):
    return True
