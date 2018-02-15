from __future__ import unicode_literals

from django.db import models
from django.utils.encoding import python_2_unicode_compatible
from django.contrib.postgres.operations import HStoreExtension
from django.contrib.postgres.fields import HStoreField

@python_2_unicode_compatible
class User(models.Model):
    """User of the JASMIN data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA client via a
    HTTP API.
    :var models.CharField name: the user name / id of the user
    :var models.EmailField email: email address of the user
    :var models.BooleanField notify: whether to notify the user when the migrations to / from storage have completed, default = True"""

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

       - ** PUT_PENDING (1) The directory has been locked and the request is queued to transfer ** TO ** storage

       - ** PUTTING (2) The directory is currently being transferred ** TO ** storage

       - ** VERIFY_PENDING (3) The directory is queued for VERIFYING

       - ** VERIFYING (4) The files are being fetched back from storage and compared to their SHA256 digests

       - ** ONSTORAGE (5) The directory is on storage

       - ** GET_PENDING (6) The directory has been locked and the request is queued to transfer ** FROM ** storage

       - ** GETTING (7) The directory is currently being transferred ** FROM ** storage

       - ** FAILED (8)
    """

    # default backend
    default_backend = "elastictape"

    # user that the directory belongs to
    user = models.ForeignKey(User, on_delete=models.CASCADE,
                             help_text="User that the directory belongs to")

    # workspace that the user wishes to use
    workspace = models.CharField(max_length=2024,
                                 help_text="Workspace used for this request")

    # stages for storage transfer - to storage.  There is a strict one to one mapping
    # for the data->storage record, and therefore a one to one mapping for the
    # data and the PUT migration.
    # For the GET migration, there may be many requests for one migration and
    # so the GET stages are in the migration request model
    ON_DISK = 0
    PUT_PENDING = 1
    PUTTING = 2
    VERIFY_PENDING = 3
    VERIFY_GETTING = 4
    VERIFYING = 5
    ON_STORAGE = 6
    FAILED = 7

    STAGE_CHOICES = ((ON_DISK, 'ON_DISK'),
                     (PUT_PENDING, 'PUT_PENDING'),
                     (PUTTING, 'PUTTING'),
                     (VERIFY_PENDING, 'VERIFY_PENDING'),
                     (VERIFY_GETTING, 'VERIFY_GETTING'),
                     (VERIFYING, 'VERIFYING'),
                     (ON_STORAGE, 'ON_STORAGE'),
                     (FAILED, 'FAILED'))
    STAGE_LIST = ['ON_DISK', 'PUT_PENDING', 'PUTTING', 'VERIFY_PENDING',
                  'VERIFY_GETTING', 'VERIFYING', 'ON_STORAGE', 'FAILED']
    stage = models.IntegerField(choices=STAGE_CHOICES, default=FAILED)

    # batch id for external storage
    external_id = models.IntegerField(blank=True,null=True,
                                      help_text="Batch id for external backup system, e.g. elastic tape or object store")

    # label - defaults to path of the directory - relative to the GWS
    label = models.CharField(blank=True, null=True, max_length=2024,
                             help_text="Human readable label for request")

    # date - the date that the directory was registered with the JDMA
    registered_date = models.DateTimeField(blank=True, null=True,
                                           help_text="Date the request was registered with the JDMA")

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

    # the backend external storage location of the migration
    storage = models.CharField(blank=False, null=False, max_length=256,
                               default=default_backend,
                               help_text="External storage location of the migration, e.g. elastictape or objectstore")

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
    MIGRATE = 2

    __REQUEST_CHOICES = ((PUT, 'PUT'),
                         (GET, 'GET'),
                         (MIGRATE, 'MIGRATE'))
    REQUEST_MAP = {"PUT" : PUT,
                   "GET" : GET,
                   "MIGRATE" : MIGRATE}
    request_type = models.IntegerField(choices=__REQUEST_CHOICES)

    ON_STORAGE = 0
    GET_PENDING = 1
    GETTING = 2
    ON_DISK = 3
    FAILED  = 4

    REQ_STAGE_CHOICES = ((ON_STORAGE, 'ON_STORAGE'),
                         (GET_PENDING, 'GET_PENDING'),
                         (GETTING, 'GETTING'),
                         (ON_DISK, 'ON_DISK'),
                         (FAILED, 'FAILED'))
    REQ_STAGE_LIST = ['ON_STORAGE', 'GET_PENDING', 'GETTING', 'ON_DISK', 'FAILED']

    stage = models.IntegerField(choices=REQ_STAGE_CHOICES, default=ON_STORAGE)

    # user that the request belongs to
    user = models.ForeignKey(User, on_delete=models.CASCADE,
                             help_text="User that the request belongs to")

    # date - the date that the request was registered with the JDMA
    date = models.DateTimeField(blank=True, null=True,
                                help_text="Date the request was registered with the JDMA")

    # target directory for GET requests - where should we put it?
    target_path = models.CharField(max_length=2024, null=True, blank=True,
                                   help_text="Target directory path")

    # mapping to the migration details
    migration = models.ForeignKey(Migration, on_delete=models.CASCADE,
                                  null=True,
                                  help_text="Migration associated with this MigrationRequest")

    # the backend external storage location of the migration - only need for PUT and MIGRATE requests
    storage = models.CharField(blank=False, null=False, max_length=256,
                               default=Migration.default_backend,
                               help_text="External storage location of the migration, e.g. elastictape or objectstore")

    # backend external storage credentials - keep as a hstore - i.e. key value store
    credentials = HStoreField(null=True,
                              help_text="Credentials required to access the external storage (encrypted)")

    # failure reason
    failure_reason = models.CharField(blank=True, null=True, max_length=1024,
                                      help_text="Reason for failure of request")

    def __str__(self):
        return "{:>4} : {:16}".format(self.pk, self.request_type)


@python_2_unicode_compatible
class MigrationFile(models.Model):
    """A record of a file in a migration in the JASMIN data migration app (JDMA)."""
    # path to the file
    path = models.CharField(max_length=2024, help_text="Absolute path to the file")
    # foreign key to Migration
    migration = models.ForeignKey(Migration, on_delete=models.CASCADE,
                                  null=True,
                                  help_text="Migration associated with this MigrationFile")
    # SHA-256 digest
    digest = models.CharField(max_length=64, help_text="SHA-256 digest of the file")

    def __str__(self):
        return self.path
