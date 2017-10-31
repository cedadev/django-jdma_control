from __future__ import unicode_literals

from django.db import models
from taggit.managers import TaggableManager
from django.utils.encoding import python_2_unicode_compatible


@python_2_unicode_compatible
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

    def __str__(self):
        return "%s (%s)" % (self.name, self.email)


@python_2_unicode_compatible
class Migration(models.Model):
    """A data model to store the details of a directory that has been migrated via the JASMIN
    data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA client via a
    HTTP API.
    :var models.IntegerField stage: The stage that the directory is at, one of:

       - ** ONDISK (0) The directory is on disk and is writable by the user

       - ** PUT_PENDING (1) The directory has been locked and the request is queued to transfer ** TO ** tape

       - ** PUTTING (2) The directory is currently being transferred ** TO ** tape

       - ** VERIFY_PENDING (3) The directory is queued for VERIFYING

       - ** VERIFYING (4) The files are being fetched back from tape and compared to their SHA256 digests

       - ** ONTAPE (5) The directory is on tape

       - ** GET_PENDING (6) The directory has been locked and the request is queued to transfer ** FROM ** tape

       - ** GETTING (7) The directory is currently being transferred ** FROM ** tape

       - ** FAILED (8)
    """


    # user that the directory belongs to
    user = models.ForeignKey(User, help_text="User that the directory belongs to")

    # workspace that the user wishes to use
    workspace = models.CharField(max_length=2024,
                                 help_text="Workspace used for this request")

    # stages for tape transfer - to tape.  There is a strict one to one mapping
    # for the data->tape record, and therefore a one to one mapping for the
    # data and the PUT migration.
    # For the GET migration, there may be many requests for one migration and
    # so the GET stages are in the migration request model
    ON_DISK = 0
    PUT_PENDING = 1
    PUTTING = 2
    VERIFY_PENDING = 3
    VERIFY_GETTING = 4
    VERIFYING = 5
    ON_TAPE = 6
    FAILED = 7

    STAGE_CHOICES = ((ON_DISK, 'ON_DISK'),
                     (PUT_PENDING, 'PUT_PENDING'),
                     (PUTTING, 'PUTTING'),
                     (VERIFY_PENDING, 'VERIFY_PENDING'),
                     (VERIFY_GETTING, 'VERIFY_GETTING'),
                     (VERIFYING, 'VERIFYING'),
                     (ON_TAPE, 'ON_TAPE'),
                     (FAILED, 'FAILED'))
    STAGE_LIST = ['ON_DISK', 'PUT_PENDING', 'PUTTING', 'VERIFY_PENDING',
                  'VERIFY_GETTING', 'VERIFYING', 'ON_TAPE', 'FAILED']
    stage = models.IntegerField(choices=STAGE_CHOICES, default=FAILED)

    # CHOICES for the permissions for batches
    PERMISSION_PRIVATE = 0      # only the user can download / request the migrations
    PERMISSION_GROUP = 1        # anyone in the group workspace can request
    PERMISSION_ALL = 2          # anyone can request it

    PERMISSION_CHOICES = ((PERMISSION_PRIVATE, 'PRIVATE'),
                          (PERMISSION_GROUP, 'GROUP'),
                          (PERMISSION_ALL, 'ALL'))
    PERMISSION_LIST = ['PRIVATE', 'GROUP', 'ALL']
    permission = models.IntegerField(choices=PERMISSION_CHOICES, default=PERMISSION_PRIVATE)

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

    # failure reason
    failure_reason = models.CharField(blank=True, null=True, max_length=1024,
                                      help_text="Reason for failure of request")


    def __str__(self):
        if self.label:
            return "{:>4} : {:16}".format(self.pk, self.label)
        else:
            return "{:>4}".format(self.pk)


@python_2_unicode_compatible
class MigrationRequest(models.Model):
    """A request to migrate (PUT) or retrieve (GET) a directory via the JASMIN data migration app (JDMA)."""
    # request type - GET or PUT or ?VERIFY?
    PUT = 0
    GET = 1

    __REQUEST_CHOICES = ((PUT, 'PUT'),
                         (GET, 'GET'))
    REQUEST_MAP = {"PUT" : PUT,
                   "GET" : GET}
    request_type = models.IntegerField(choices=__REQUEST_CHOICES)

    ON_TAPE = 0
    GET_PENDING = 1
    GETTING = 2
    ON_DISK = 3
    FAILED  = 4

    REQ_STAGE_CHOICES = ((ON_TAPE, 'ON_TAPE'),
                         (GET_PENDING, 'GET_PENDING'),
                         (GETTING, 'GETTING'),
                         (ON_DISK, 'ON_DISK'),
                         (FAILED, 'FAILED'))
    REQ_STAGE_LIST = ['ON_TAPE', 'GET_PENDING', 'GETTING', 'ON_DISK', 'FAILED']

    stage = models.IntegerField(choices=REQ_STAGE_CHOICES, default=ON_TAPE)

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

    # failure reason
    failure_reason = models.CharField(blank=True, null=True, max_length=1024,
                                      help_text="Reason for failure of request")

    def __str__(self):
        return "{:>4} : {:16}".format(self.pk, self.request_type)
