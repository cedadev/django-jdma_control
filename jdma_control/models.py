from __future__ import unicode_literals

from django.db import models
from taggit.managers import TaggableManager

class User(models.Model):
    """User of the JASMIN data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA client via a
    HTTP API.
    :var models.CharField name: the user name / id of the user
    :var models.EmailField email: email address of the user
    :var models.BooleanField notify: whether to notify the user when the migrations to / from tape have completed, default = True"""

    name = models.CharField(max_length=254, help_text="Name of user - should be same as JASMIN user name")
    email = models.EmailField(max_length=254, help_text="Email of user")
    notify = models.BooleanField(default=True, help_text="Switch notifications on / off")

    def __unicode__(self):
        return "%s (%s)" % (self.name, self.email)


class Migration(models.Model):
    """A data model to store the details of a directory that has been migrated via the JASMIN
    data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA client via a
    HTTP API.
    :var models.IntegerField stage: The stage that the directory is at, one of (DLPTMGF):

       - ** D ** ONDISK (0) The directory is on disk and is writable by the user

       - ** L ** LOCKED_PUT (1) The directory has been locked and the request is queued to transfer ** TO ** tape

       - ** P ** TAPE_PUT (2) The directory is currently being transferred ** TO ** tape

       - ** T ** ONTAPE (3) The directory is on tape

       - ** M ** LOCKED_GET (4) The directory has been locked and the request is queued to transfer ** FROM ** tape

       - ** G ** TAPE_GET (5) The directory is currently being transferred ** FROM ** tape

       - ** F ** FAILED (6)
    """


    # user that the directory belongs to
    user = models.ForeignKey(User, help_text="User that the directory belongs to")

    # workspace that the user wishes to use
    workspace = models.CharField(max_length=2024,
                                 help_text="Workspace used for this request")

    # stages for tape transfer
    ON_DISK = 0
    PUT_PENDING = 1
    PUTTING = 2
    ON_TAPE = 3
    GET_PENDING = 4
    GETTING = 5
    FAILED = 6

    __STAGE_CHOICES = ((ON_DISK, 'ON_DISK'),
                       (PUT_PENDING, 'PUT_PENDING'),
                       (PUTTING, 'PUTTING'),
                       (ON_TAPE, 'ON_TAPE'),
                       (GET_PENDING, 'GET_PENDING'),
                       (GETTING, 'GETTING'),
                       (FAILED, 'FAILED'))
    STAGE_CHOICES = __STAGE_CHOICES
    stage = models.IntegerField(choices=__STAGE_CHOICES)

    # batch id for elastic tape
    et_id = models.IntegerField(blank=True,null=True,
                                help_text="Elastic tape batch id")

    # label - defaults to path of the directory - relative to the GWS
    label = models.CharField(blank=True, null=True, max_length=2024,
                             help_text="Human readable label for request")

    # date - the date that the directory was registered with the JDMA
    registered_date = models.DateTimeField(blank=True, null=True,
                                           help_text="Date the request was registered with the JDMA")

    # tags - using django-taggit pypi package
    tags = TaggableManager(blank=True)

    # original directory - for restoring straight back
    original_path = models.CharField(max_length=2024, unique=True,
                                     help_text="Original directory path")

    # user id, group id and permissions - record what they are so that they can be restored when the directory is restored
    unix_user_id = models.CharField(blank=True, null=True, max_length=256,
                                    help_text="uid of original owner of directory")
    unix_group_id = models.CharField(blank=True, null=True, max_length=256,
                                     help_text="gid of original owner of directory")
    unix_permission = models.IntegerField(blank=True, null=True,
                                          help_text="File permissions of original directory")

    def __unicode__(self):
        if self.label:
            return "{:>4} : {:16}".format(self.pk, self.label)
        else:
            return "{:>4}".format(self.pk)


class MigrationRequest(models.Model):
    """A request to migrate (PUT) or retrieve (GET) a directory via the JASMIN data migration app (JDMA)."""
    # request type - GET or PUT or ?VERIFY?
    PUT = 0
    GET = 1
    VERIFY = 2

    __REQUEST_CHOICES = ((PUT, 'PUT'),
                         (GET, 'GET'),
                         (VERIFY, 'VERIFY'))
    REQUEST_MAP = {"PUT" : PUT,
                   "GET" : GET,
                   "VERIFY" : VERIFY}
    request_type = models.IntegerField(choices=__REQUEST_CHOICES)

    # user that the request belongs to
    user = models.ForeignKey(User, help_text="User that the request belongs to")

    # date - the date that the request was registered with the JDMA
    date = models.DateTimeField(blank=True, null=True,
                                help_text="Date the request was registered with the JDMA")

    # target directory for GET requests - where should we put it?
    target_path = models.CharField(max_length=2024, null=True, blank=True,
                                   help_text="Target directory path")

    # mapping to the migration details
    migration = models.ForeignKey(Migration, null=True,
                                  help_text="Migration associated with this MigrationRequest")
