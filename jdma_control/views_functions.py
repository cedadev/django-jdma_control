"""Support functions for the views.py"""

from django.http import HttpResponse
import os
import json
import pwd, grp

def HttpError(error_data, status=404):
    return HttpResponse(json.dumps(error_data),
                        content_type="application/json", status=status, reason=error_data["error"])


def UserHasWritePermission(path, user):
    """Determine whether a particular user has write permission to a directory / file at path
       The rules used to determine are:
         1. If "all" have write permission -> True
         2. If "group" has write permission and the user is a member of the group -> True
         3. If "user" has write permission and the user is the owner -> True
         4. -> False
    """
    # get the file status, covert to octal and mask to the lower 3 octal values
    fstat = os.stat(path)
    mode = fstat.st_mode & 0o777
    # check for all
    if mode & 0o002:
        return True
    # check for group
    if mode & 0o020:
        # now we need to check that user is part of the group that owns the file at path
        group = grp.getgrgid(fstat.st_gid)
        if user in group.gr_mem:
            return True
    # check for user
    if mode & 0o200:
        # check that the owner of the file matches the user
        # get the uid of the user
        user_uid = pwd.getpwnam(user).pw_uid
        if fstat.st_uid == user_uid:
            return True
    return False


def UserHasETQuota(path, user, workspace):
    return True

def UserHasSufficientDiskSpace(path, user, size):
    return True
