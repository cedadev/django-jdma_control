from __future__ import unicode_literals

from django.db import models
from django.utils.encoding import python_2_unicode_compatible
from django.contrib.postgres.operations import HStoreExtension
from django.contrib.postgres.fields import HStoreField, ArrayField
from sizefield.models import FileSizeField
from sizefield.utils import filesizeformat
import jdma_control.backends

@python_2_unicode_compatible
class User(models.Model):
    """User of the JASMIN data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA
    client via a HTTP API.
    :var models.CharField name: the user name / id of the user
    :var models.EmailField email: email address of the user
    :var models.BooleanField notify: whether to notify the user when the
    migrations to / from storage have completed, default = True
    """

    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="Name of user - should be same as JASMIN user name"
    )

    email = models.EmailField(
        max_length=256,
        help_text="Email of user"
    )

    notify = models.BooleanField(
        default=True,
        help_text="Switch notifications on / off"
    )

    def __str__(self):
        return "%s (%s)" % (self.name, self.email)


@python_2_unicode_compatible
class Groupworkspace(models.Model):
    """A record of a quota for a group workspace,
    for each external storage backend
    """

    workspace = models.CharField(
        max_length=1024,
        help_text="Name of groupworkspace (GWS)",
        null=False,
        blank=False
    )

    path = models.CharField(
        max_length=1024,
        help_text="Path of groupworkspace (optional)",
        null=True,
        blank=True
    )

    managers = models.ManyToManyField(
        User,
        help_text="Managers of this groupworkspace"
    )

    def __str__(self):
        return self.workspace


@python_2_unicode_compatible
class StorageQuota(models.Model):
    """Storage type and quota to be used by Groupworkspace Quota"""

    # populate choices from backends
    bi = 0
    STORAGE=[]
    __STORAGE_CHOICES = []
    for be in jdma_control.backends.get_backends():
        bo = be()
        __STORAGE_CHOICES.append((bi, bo.get_id()))
        STORAGE.append(bo.get_id())
        bi += 1
    storage = models.IntegerField(choices=__STORAGE_CHOICES, default=0)

    quota_size = FileSizeField(
        default=0,
        help_text="Size of quota allocated to the groupworkspace"
    )

    quota_used = FileSizeField(
        default=0,
        help_text="Size of quota used in the groupworkspace"
    )

    # keep a record of the workspace so we can search on it
    workspace = models.ForeignKey(
        Groupworkspace, null=False,
        help_text="Workspace that this storage quota is for",
        on_delete=models.CASCADE
    )

    def get_storage_name(nid):
        """Get the storage name from the numerical id"""
        return StorageQuota.STORAGE[nid]

    def get_storage_index(name):
        return StorageQuota.STORAGE.index(name)

    def quota_formatted_used(self):
        return filesizeformat(self.quota_used)
    quota_formatted_used.short_description = "quota_used"

    def quota_formatted_size(self):
        return filesizeformat(self.quota_size)
    quota_formatted_size.short_description = "quota_size"

    def get_name(self):
        return StorageQuota.__STORAGE_CHOICES[self.storage][1]
    get_name.short_description = "quota_name"

    def __str__(self):
        desc_str = "{} : {} : {} / {}".format(
            self.workspace.workspace,
            str(StorageQuota.__STORAGE_CHOICES[self.storage][1]),
            filesizeformat(self.quota_used),
            filesizeformat(self.quota_size)
        )
        return desc_str

@python_2_unicode_compatible
class Migration(models.Model):
    """A data model to store the details of a directory that has been migrated
    via the JASMIN data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA
    client via a HTTP API.
    :var models.IntegerField stage: The stage that the directory is at, one of:

       - ** ONDISK (0) The directory is on disk and is writable by the user

       - ** PUT_PENDING (1) The directory has been locked and the request is
       queued to transfer ** TO ** storage

       - ** PUTTING (2) The directory is currently being transferred ** TO **
       storage

       - ** VERIFY_PENDING (3) The directory is queued for VERIFYING

       - ** VERIFYING (4) The files are being fetched back from storage and
       compared to their SHA256 digests

       - ** ONSTORAGE (5) The directory is on storage

       - ** GET_PENDING (6) The directory has been locked and the request is
       queued to transfer ** FROM ** storage

       - ** GETTING (7) The directory is currently being transferred ** FROM **
       storage

       - ** FAILED (8)
    """

    # default backend
    default_backend = "elastictape"

    # user that the directory belongs to
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=False,
                             help_text="User that the migration belongs to")

    # workspace that the user wishes to use
    workspace = models.ForeignKey(Groupworkspace, null=False,
                                  help_text="Workspace used for this request",
                                  on_delete=models.CASCADE)

    # stages for storage transfer - to storage.  There is a strict one to one
    # mapping for the data->storage record, and therefore a one to one mapping
    # for the data and the PUT migration.
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
    external_id = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text=("Batch id for external backup system, "
                   "e.g. elastic tape or object store")
    )

    # label - defaults to path of the directory - relative to the GWS
    label = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text="Human readable label for request"
    )

    # date - the date that the directory was registered with the JDMA
    registered_date = models.DateTimeField(
        blank=True,
        null=True,
        help_text="Date the request was registered with the JDMA"
    )

    # filelist - for uploading / downloading lists of files
    filelist = ArrayField(
        models.CharField(max_length=1024, unique=True, blank=True),
        blank=True,
        null=True,
        help_text="List of files for uploading or downloading"
    )

    # the backend external storage location of the migration
    storage = models.ForeignKey(
        StorageQuota,
        on_delete=models.CASCADE,
        null=False,
        help_text=("External storage location of the migration, "
                   "e.g. elastictape or objectstore")
    )

    def __str__(self):
        if self.label:
            return "{:>4} : {:16}".format(self.pk, self.label)
        else:
            return "{:>4}".format(self.pk)

    def name(self):
        return self.__str__()

    def formatted_filelist(self):
        """Return a string of the filelist separated by linebreaks, rather than
        commas.
        Put the output in a Textarea widget in the Admin form
        """
        out_str = ""
        if self.filelist:
            for f in self.filelist:
                out_str += f + "\n"
            return out_str
    formatted_filelist.short_description = "filelist"


@python_2_unicode_compatible
class MigrationRequest(models.Model):
    """A request to migrate (PUT) or retrieve (GET) a directory via the JASMIN
    data migration app (JDMA).
    """

    # request type - GET or PUT or ?VERIFY?
    PUT = 0
    GET = 1
    MIGRATE = 2

    __REQUEST_CHOICES = ((PUT, 'PUT'),
                         (GET, 'GET'),
                         (MIGRATE, 'MIGRATE'))
    REQUEST_MAP = {"PUT": PUT,
                   "GET": GET,
                   "MIGRATE": MIGRATE}
    request_type = models.IntegerField(choices=__REQUEST_CHOICES)

    ON_STORAGE = 0
    GET_PENDING = 1
    GETTING = 2
    ON_DISK = 3
    FAILED = 4
    PUTTING = 5

    REQ_STAGE_CHOICES = ((ON_STORAGE, 'ON_STORAGE'),
                         (PUTTING, 'PUTTING'),
                         (GET_PENDING, 'GET_PENDING'),
                         (GETTING, 'GETTING'),
                         (ON_DISK, 'ON_DISK'),
                         (FAILED, 'FAILED'))

    REQ_STAGE_LIST = ['ON_STORAGE',
                      'GET_PENDING',
                      'GETTING',
                      'ON_DISK',
                      'FAILED']

    stage = models.IntegerField(
        choices=REQ_STAGE_CHOICES,
        default=ON_STORAGE,
        help_text="Current upload / download stage"
    )

    # user that the request belongs to
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        help_text="User that the request belongs to"
    )

    # date - the date that the request was registered with the JDMA
    date = models.DateTimeField(
        blank=True,
        null=True,
        help_text="Date the request was registered with the JDMA"
    )

    # target directory for GET requests - where should we put it?
    target_path = models.CharField(
        max_length=1024,
        null=True,
        blank=True,
        help_text="Target directory path"
    )

    # mapping to the migration details
    migration = models.ForeignKey(
        Migration,
        on_delete=models.CASCADE,
        null=True,
        help_text="Migration associated with this MigrationRequest"
    )

    # backend external storage credentials - keep as a hstore -
    # i.e. key value store
    credentials = HStoreField(
        null=True,
        blank=True,
        help_text=("Credentials required to access the external storage "
                   "(encrypted)")
    )

    # failure reason
    failure_reason = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text="Reason for failure of request"
    )

    # last archive to be uploaded / downloaded successfully - this should
    # allow resumption of uploads / downloads
    last_archive = models.IntegerField(
        default=0,
        help_text="Last completed uploaded / downloaded archive"
    )

    def __str__(self):
        return "{:>4} : {:16}".format(self.pk, self.request_type)


@python_2_unicode_compatible
class MigrationArchive(models.Model):
    """An archive stores a list of files that are to be tarred together then
    uploaded.
    This is to enabled efficient upload / download of small files.
    An archive may often contain only one file.
    """
    # SHA-256 digest
    digest = models.CharField(
        max_length=64,
        help_text="SHA-256 digest of the archive"
    )
    # which migration does this belong to?
    # Many to one mapping (many Migration Archives->one Migration)
    migration = models.ForeignKey(
        Migration,
        on_delete=models.CASCADE,
        null=False,
        help_text="Migration that this Archive belongs to"
    )

    def name(self):
        return "Archive " + str(self.pk)
    name.short_description = "archive_name"

    def get_id(self):
        return "archive_{:010}".format(self.pk)
    name.short_description = "get_id"

    def first_file(self):
        """Get the first file in the archive"""
        q_set = self.migrationfile_set.all()
        if len(q_set) == 0:
            return ""
        else:
            fname = q_set[0].path
            return str(len(q_set)) + " files. First file: " + fname
    first_file.short_description = "first_file"

    def __str__(self):
        """Return a string representation"""
        # get the migration
        return "Archive " + str(self.pk)


@python_2_unicode_compatible
class MigrationFile(models.Model):
    """A record of a file in a migration in the JASMIN data migration app
    (JDMA).
    """
    # path to the file
    path = models.CharField(
        max_length=1024,
        null=True,
        help_text="Absolute path to the file"
    )
    # SHA-256 digest
    digest = models.CharField(
        max_length=64,
        null=True,
        help_text="SHA-256 digest of the file"
    )
    # size in bytes
    size = FileSizeField(
        null=False,
        default=0,
        help_text="size of file in bytes"
    )

    # user id, group id and permissions - record what they are so that they
    # can be restored when the directory is restored
    unix_user_id = models.CharField(
        blank=True,
        null=True,
        max_length=256,
        help_text="uid of original owner of directory"
    )
    unix_group_id = models.CharField(
        blank=True,
        null=True,
        max_length=256,
        help_text="gid of original owner of directory"
    )
    unix_permission = models.IntegerField(
        blank=True,
        null=True,
        help_text="File permissions of original directory"
    )
    # which archive does this file belong to?
    archive = models.ForeignKey(
        MigrationArchive,
        on_delete=models.CASCADE,
        null=False,
        help_text="Archive that this File belongs to"
    )

    def formatted_size(self):
        return filesizeformat(self.size)
    formatted_size.short_description = "formatted_size"

    def __str__(self):
        return self.path
