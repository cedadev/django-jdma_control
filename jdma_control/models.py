from __future__ import unicode_literals

from django.db import models
from django.contrib.postgres.operations import HStoreExtension
from django.contrib.postgres.fields import HStoreField, ArrayField
from sizefield.models import FileSizeField
from sizefield.utils import filesizeformat
import jdma_control.backends
import os


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
        help_text="Name of user - should be same as JASMIN user name",
        db_index=True,
    )

    email = models.EmailField(max_length=256, help_text="Email of user")

    notify = models.BooleanField(
        default=True, help_text="Switch notifications on / off"
    )

    def __str__(self):
        return "%s (%s)" % (self.name, self.email)


class Groupworkspace(models.Model):
    """A record of a quota for a group workspace,
    for each external storage backend
    """

    workspace = models.CharField(
        max_length=1024,
        help_text="Name of groupworkspace (GWS)",
        null=False,
        blank=False,
    )

    path = models.CharField(
        max_length=1024,
        help_text="Path of groupworkspace (optional)",
        null=True,
        blank=True,
    )

    managers = models.ManyToManyField(User, help_text="Managers of this groupworkspace")

    def __str__(self):
        return self.workspace


class StorageQuota(models.Model):
    """Storage type and quota to be used by Groupworkspace Quota"""

    # populate choices from backends
    bi = 0
    STORAGE = []
    __STORAGE_CHOICES = []
    for be in jdma_control.backends.get_backends():
        bo = be()
        __STORAGE_CHOICES.append((bi, bo.get_id()))
        STORAGE.append(bo.get_id())
        bi += 1
    storage = models.IntegerField(choices=__STORAGE_CHOICES, default=0, db_index=True)

    quota_size = FileSizeField(
        default=0, help_text="Size of quota allocated to the groupworkspace"
    )

    quota_used = FileSizeField(
        default=0, help_text="Size of quota used in the groupworkspace"
    )

    # keep a record of the workspace so we can search on it
    workspace = models.ForeignKey(
        Groupworkspace,
        null=False,
        help_text="Workspace that this storage quota is for",
        on_delete=models.CASCADE,
    )

    def get_storage_name(nid):
        """Get the storage name from the numerical id"""
        return StorageQuota.STORAGE[nid]

    def get_storage_index(name):
        return StorageQuota.STORAGE.index(name)

    def quota_formatted_used(self):
        return filesizeformat(self.quota_used)

    quota_formatted_used.short_description = "Quota used"

    def quota_formatted_size(self):
        return filesizeformat(self.quota_size)

    quota_formatted_size.short_description = "Quota size"

    def get_name(self):
        return StorageQuota.__STORAGE_CHOICES[self.storage][1]

    get_name.short_description = "quota_name"

    def __str__(self):
        desc_str = "{} : {} : {} / {}".format(
            self.workspace.workspace,
            str(StorageQuota.__STORAGE_CHOICES[self.storage][1]),
            filesizeformat(self.quota_used),
            filesizeformat(self.quota_size),
        )
        return desc_str


class Migration(models.Model):
    """A data model to store the details of a directory that has been migrated
    via the JASMIN data migration app (JDMA).
    Users register directories from a group workspace (GWS) with the JDMA
    client via a HTTP API.
    :var models.IntegerField stage: The stage that the directory is at.
    """

    # user that the directory belongs to
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        null=False,
        help_text="User that the migration belongs to",
    )

    # workspace that the user wishes to use
    workspace = models.ForeignKey(
        Groupworkspace,
        null=False,
        help_text="Workspace used for this request",
        on_delete=models.CASCADE,
    )

    # states of the Migration.
    # There are now only three states - ON_DISK, ON_STORAGE and FAILED
    # The Migration is ON_DISK until it has been fully uploaded to the external
    #  storage and deleted.
    ON_DISK = 0
    PUTTING = 1
    ON_STORAGE = 2
    FAILED = 3
    DELETING = 4
    DELETED = 5

    STAGE_CHOICES = (
        (ON_DISK, "ON_DISK"),
        (PUTTING, "PUTTING"),
        (ON_STORAGE, "ON_STORAGE"),
        (FAILED, "FAILED"),
        (DELETING, "DELETING"),
        (DELETED, "DELETED"),
    )
    STAGE_LIST = ["ON_DISK", "PUTTING", "ON_STORAGE", "FAILED", "DELETING", "DELETED"]
    stage = models.IntegerField(choices=STAGE_CHOICES, default=FAILED)

    # batch id for external storage
    external_id = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text=(
            "Batch id for external backup system, " "e.g. elastic tape or object store"
        ),
    )

    # label - defaults to path of the directory - relative to the GWS
    label = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text="Human readable label for request",
    )

    # date - the date that the directory was registered with the JDMA
    registered_date = models.DateTimeField(
        blank=True, null=True, help_text="Date the request was registered with the JDMA"
    )

    # common path - the common path for the files, as found by
    # os.path.commonprefix
    common_path = models.CharField(
        blank=True,
        null=True,
        max_length=2048,
        help_text="Common path prefix for all files in the filelist",
    )
    # gid, uid and file permissions for common_path (cp)
    common_path_user_id = models.IntegerField(
        blank=True,
        null=True,
        help_text="uid of original owner of common_path directory",
    )
    common_path_group_id = models.IntegerField(
        blank=True,
        null=True,
        help_text="gid of original owner of common_path directory",
    )
    common_path_permission = models.IntegerField(
        blank=True,
        null=True,
        help_text="File permissions of original common_path directory",
    )

    # the backend external storage location of the migration
    storage = models.ForeignKey(
        StorageQuota,
        on_delete=models.CASCADE,
        null=False,
        help_text=(
            "External storage location of the migration, "
            "e.g. elastictape or objectstore"
        ),
    )

    def __str__(self):
        if self.label:
            return "{:>4} : {:16}".format(self.pk, self.label)
        else:
            return "{:>4}".format(self.pk)

    def name(self):
        return self.__str__()

    def get_id(self):
        return "migration_{:08}".format(self.pk)


class MigrationRequest(models.Model):
    """A request to upload (PUT) or retrieve (GET) a directory via the JASMIN
    data migration app (JDMA).
    """

    # request type - GET or PUT or ?VERIFY?
    PUT = 0
    GET = 1
    MIGRATE = 2
    DELETE = 3

    __REQUEST_CHOICES = (
        (PUT, "PUT"),
        (GET, "GET"),
        (MIGRATE, "MIGRATE"),
        (DELETE, "DELETE"),
    )
    REQUEST_MAP = {"PUT": PUT, "GET": GET, "MIGRATE": MIGRATE, "DELETE": DELETE}
    REQUEST_LIST = {PUT: "PUT", GET: "GET", MIGRATE: "MIGRATE", DELETE: "DELETE"}
    request_type = models.IntegerField(choices=__REQUEST_CHOICES, db_index=True)

    # new state machine for GET / PUT / MIGRATE
    # all transactions occur through the MigrationRequest, rather than a hybrid
    # of the Migration and MigrationRequest
    PUT_START = 0
    PUT_BUILDING = 1
    PUT_PENDING = 2
    PUT_PACKING = 3
    PUTTING = 4
    VERIFY_PENDING = 5
    VERIFY_GETTING = 6
    VERIFYING = 7
    PUT_TIDY = 8
    PUT_COMPLETED = 9

    GET_START = 100
    GET_PENDING = 101
    GETTING = 102
    GET_UNPACKING = 103
    GET_RESTORE = 104
    GET_TIDY = 105
    GET_COMPLETED = 106

    DELETE_START = 200
    DELETE_PENDING = 201
    DELETING = 202
    DELETE_TIDY = 203
    DELETE_COMPLETED = 204

    FAILED = 1000
    FAILED_COMPLETED = 1001

    REQ_STAGE_CHOICES = (
        (PUT_START, "PUT_START"),
        (PUT_BUILDING, "PUT_BUILDING"),
        (PUT_PENDING, "PUT_PENDING"),
        (PUT_PACKING, "PUT_PACKING"),
        (PUTTING, "PUTTING"),
        (VERIFY_PENDING, "VERIFY_PENDING"),
        (VERIFY_GETTING, "VERIFY_GETTING"),
        (VERIFYING, "VERIFYING"),
        (PUT_TIDY, "PUT_TIDY"),
        (PUT_COMPLETED, "PUT_COMPLETED"),
        (GET_START, "GET_START"),
        (GET_PENDING, "GET_PENDING"),
        (GETTING, "GETTING"),
        (GET_UNPACKING, "GET_UNPACKING"),
        (GET_RESTORE, "GET_RESTORE"),
        (GET_TIDY, "GET_TIDY"),
        (GET_COMPLETED, "GET_COMPLETED"),
        (DELETE_START, "DELETE_START"),
        (DELETE_PENDING, "DELETE_PENDING"),
        (DELETING, "DELETING"),
        (DELETE_TIDY, "DELETE_TIDY"),
        (DELETE_COMPLETED, "DELETE_COMPLETED"),
        (FAILED, "FAILED"),
        (FAILED_COMPLETED, "FAILED_COMPLETED"),
    )

    REQ_STAGE_LIST = {
        PUT_START: "PUT_START",
        PUT_BUILDING: "PUT_BUILDING",
        PUT_PENDING: "PUT_PENDING",
        PUT_PACKING: "PUT_PACKING",
        PUTTING: "PUTTING",
        VERIFY_PENDING: "VERIFY_PENDING",
        VERIFY_GETTING: "VERIFY_GETTING",
        VERIFYING: "VERIFYING",
        PUT_TIDY: "PUT_TIDY",
        PUT_COMPLETED: "PUT_COMPLETED",
        GET_START: "GET_START",
        GET_PENDING: "GET_PENDING",
        GETTING: "GETTING",
        GET_UNPACKING: "GET_UNPACKING",
        GET_RESTORE: "GET_RESTORE",
        GET_TIDY: "GET_TIDY",
        GET_COMPLETED: "GET_COMPLETED",
        DELETE_START: "DELETE_START",
        DELETE_PENDING: "DELETE_PENDING",
        DELETING: "DELETING",
        DELETE_TIDY: "DELETE_TIDY",
        DELETE_COMPLETED: "DELETE_COMPLETED",
        FAILED: "FAILED",
        FAILED_COMPLETED: "FAILED_COMPLETED",
    }

    stage = models.IntegerField(
        choices=REQ_STAGE_CHOICES,
        default=FAILED,
        help_text="Current upload / download stage",
        db_index=True,
    )

    # user that the request belongs to
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, help_text="User that the request belongs to"
    )

    # date - the date that the request was registered with the JDMA
    date = models.DateTimeField(
        blank=True, null=True, help_text="Date the request was registered with the JDMA"
    )

    # target directory for GET requests - where should we put it?
    target_path = models.CharField(
        max_length=1024, null=True, blank=True, help_text="Target directory path"
    )

    # mapping to the migration details
    migration = models.ForeignKey(
        Migration,
        on_delete=models.CASCADE,
        null=True,
        help_text="Migration associated with this MigrationRequest",
    )

    # backend external storage credentials - keep as a hstore -
    # i.e. key value store
    credentials = HStoreField(
        null=True,
        blank=True,
        help_text=(
            "Credentials required to access the external storage " "(encrypted)"
        ),
    )

    # failure reason
    failure_reason = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text="Reason for failure of request",
    )

    # filelist - for uploading / downloading lists of files
    filelist = ArrayField(
        models.CharField(max_length=1024, unique=True, blank=True),
        blank=True,
        null=True,
        help_text="List of files for uploading or downloading",
    )

    # last archive to be uploaded / downloaded successfully - this should
    # allow resumption of uploads / downloads
    last_archive = models.IntegerField(
        default=0, help_text="Last completed uploaded / downloaded archive"
    )

    # transfer id for external storage - might not be neccessary for all, but
    # required for elastic tape
    transfer_id = models.CharField(
        blank=True,
        null=True,
        max_length=1024,
        help_text=("Tranfer id for external backup system, " "e.g. elastic tape"),
    )

    # whether the MigrationRequest is locked or not
    # this allows different backend storage systems to reside on different
    # servers and run multiple instances, without causing race conditions
    # or acting on the same migration twice
    locked = models.BooleanField(
        default=False, help_text="Migration is locked for processing"
    )

    def __str__(self):
        return "{:>4} : {:16}".format(
            self.pk, MigrationRequest.REQUEST_LIST[self.request_type]
        )

    def lock(self):
        # check that the stage isn't changed while we're accessing and waiting
        # for the db
        current_stage = self.stage
        current_locked = self.locked
        self.locked = True
        self.save()
        self.refresh_from_db()
        if self.stage != current_stage:
            self.locked = current_locked
            self.save()
            return False
        return self.lock != current_locked

    def unlock(self):
        n_updated = MigrationRequest.objects.filter(pk=self.pk, locked=True).update(
            locked=False
        )
        return bool(n_updated)

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


class MigrationArchive(models.Model):
    """An archive stores a list of files that are to be tarred together then
    uploaded.
    This is to enabled efficient upload / download of small files.
    An archive may often contain only one file.
    """

    # Checksum digest and format
    digest = models.CharField(max_length=64, help_text="Digest of the archive")
    digest_format = models.CharField(
        max_length=32, null=True, blank=False, default="SHA256"
    )
    # which migration does this belong to?
    # Many to one mapping (many Migration Archives->one Migration)
    migration = models.ForeignKey(
        Migration,
        on_delete=models.CASCADE,
        null=False,
        help_text="Migration that this Archive belongs to",
    )

    # size in bytes
    size = FileSizeField(null=False, default=0, help_text="size of file in bytes")

    # is the archive to be packed / is it packed?
    packed = models.BooleanField(
        default=False, help_text="Is the archive packed (tarred)?"
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
        if q_set.count() == 0:
            return ""
        else:
            fname = q_set.first().path
            return str(q_set.count()) + " files. First file: " + fname

    first_file.short_description = "first_file"

    def formatted_size(self):
        return filesizeformat(self.size)

    formatted_size.short_description = "size"

    def __str__(self):
        """Return a string representation"""
        # get the migration
        return "Archive " + str(self.pk)

    def get_archive_name(self, prefix=""):
        """Get the name of the archive, if the archive is packed"""
        if not self.packed:
            return ""
        else:
            return os.path.join(prefix, self.get_id() + ".tar")

    def get_file_names(self, prefix="", filter_list=None):
        """Return a dictionary of three lists of files from the archive to be
        / that have been uploaded.
        The dictionary consists of:
          {"FILE" : [list of files],
           "DIR"  : [list of directories],
           "LINK" : [list of links],
           "LNCM" : [list of links with relation to a common path],
           "LNAS" : [list of links with absolute path]}
        The function can also be given an optional filelist, to only include
        files that are in the filelist.  This is so that GET requests can
        specify a subset of files to download.
        """
        # not packed, return a list of the files in the archive
        file_list = {"FILE": [], "DIR": [], "LINK": [], "LNAS": [], "LNCM": []}
        for f in self.migrationfile_set.all():
            if filter_list is None:
                file_list[f.ftype].append(os.path.join(prefix, f.path))
            else:
                if f.path in filter_list:
                    full_path = os.path.join(prefix, f.path)
                    if not full_path in file_list[f.ftype]:
                        file_list[f.ftype].append(full_path)
        return file_list

    get_file_names.short_description = "Filelist"

    def get_file_list_text(self):
        """Convert the output of get_filtered file_names into a string buffer"""
        output = ""
        for f in self.migrationfile_set.all():
            output += f.path + " : " + f.ftype + "\n"
        return output

    get_file_list_text.short_description = "List of files in archive"


class MigrationFile(models.Model):
    """A record of a file in a migration in the JASMIN data migration app
    (JDMA).
    """

    # path to the file
    path = models.CharField(
        max_length=1024,
        null=True,
        help_text="Relative path to the file (relative to Migration.common_path)",
    )
    # Checksum digest
    digest = models.CharField(
        max_length=64, null=True, help_text="Checksum digest of the file"
    )
    digest_format = models.CharField(
        max_length=32, null=True, blank=False, default="SHA256"
    )
    # size in bytes
    size = FileSizeField(null=False, default=0, help_text="size of file in bytes")
    # file type - a string, either "FILE", "DIR", "LINK", "LNCM", "LNAS" or "MISS" (missing)
    ftype = models.CharField(
        max_length=4, null=False, default="FILE", help_text="Type of the file"
    )
    # link location - we can then restore links on restore
    link_target = models.CharField(
        max_length=1024,
        null=True,
        blank=True,
        help_text="Relative (for LNCM) and absolute (for LNAS) path to the linked file location",
    )
    # user id, group id and permissions - record what they are so that they
    # can be restored when the directory is restored
    unix_user_id = models.IntegerField(
        blank=True, null=True, help_text="uid of original owner of file"
    )
    unix_group_id = models.IntegerField(
        blank=True, null=True, help_text="gid of original owner of file"
    )
    unix_permission = models.IntegerField(
        blank=True, null=True, help_text="File permissions of original file"
    )
    # which archive does this file belong to?
    archive = models.ForeignKey(
        MigrationArchive,
        on_delete=models.CASCADE,
        null=False,
        help_text="Archive that this File belongs to",
    )

    def formatted_size(self):
        return filesizeformat(self.size)

    formatted_size.short_description = "size"

    def __str__(self):
        return "{} {}".format(self.path, self.ftype)
