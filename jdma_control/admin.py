from django.contrib import admin
from jdma_control.models import *
from django.urls import reverse
from django.utils.safestring import mark_safe


# Register your models here.
class UserAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('name', 'email', 'notify')
    fields = ('name', 'email', 'notify')
    search_fields = ('name', 'email')
admin.site.register(User, UserAdmin)


class MigrationArchiveInline(admin.TabularInline):
    model = MigrationArchive
    fields = ('first_file',)
    readonly_fields = ('first_file',)
    can_delete = False
    extra = 0
    show_change_link = True

    def has_add_permission(self, request):
        return False


class MigrationAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'label', 'user', 'stage', 'workspace',
                    'storage', 'external_id', 'registered_date')
    list_filter = ('stage', 'registered_date', 'workspace',)

    fields = ('user', 'label', 'workspace', 'stage', 'storage',
              'external_id', 'registered_date', 'formatted_filelist')
    readonly_fields = ('formatted_filelist', 'storage', 'external_id')

    search_fields = ('user', 'label', 'workspace', 'stage', 'external_id')
    inlines = [MigrationArchiveInline]
admin.site.register(Migration, MigrationAdmin)


class MigrationRequestAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'user', 'request_type', 'stage', 'date', 'migration')
    list_filter = ('request_type', 'date', 'stage',)

    fields = ('user', 'request_type', 'stage', 'date', 'migration',
              'target_path', 'credentials', 'last_archive', 'failure_reason')
    readonly_fields = ('migration', 'credentials', 'last_archive',
                       'failure_reason')
    search_fields = ('user',)
admin.site.register(MigrationRequest, MigrationRequestAdmin)


class MigrationFileAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'path', 'formatted_size', 'archive')
    fields = ('path', 'digest', 'formatted_size', 'unix_user_id',
              'unix_group_id', 'unix_permission', 'link_to_archive')
    readonly_fields = ('digest', 'formatted_size', 'link_to_archive')
    search_fields = ('path',)

    def link_to_archive(self, obj):
        link = reverse('admin:jdma_control_migrationarchive_change',
                       args=[obj.archive.pk])
        return mark_safe(u'<a href="%s">%s</a>' % (link, obj.archive.name()))
    link_to_archive.short_description = "Archive"
    link_to_archive.help_text = "Archive that this File belongs to"

admin.site.register(MigrationFile, MigrationFileAdmin)


class MigrationFileInline(admin.TabularInline):
    model = MigrationFile
    fields = ('formatted_size',)
    readonly_fields = ('formatted_size',)
    can_delete = False
    extra = 0
    show_change_link = True

    def has_add_permission(self, request):
        return False


class MigrationArchiveAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'migration', 'formatted_size', 'digest')
    fields = ('link_to_migration', 'formatted_size', 'digest')
    readonly_fields = ('link_to_migration', 'formatted_size', 'digest')
    search_fields = ('migration.workspace',)
    inlines = [MigrationFileInline]

    def link_to_migration(self, obj):
        link = reverse('admin:jdma_control_migration_change',
                       args=[obj.migration.id])
        return mark_safe(u'<a href="%s">%s</a>' % (link, obj.migration.name()))
    link_to_migration.short_description = "Migration"
    link_to_migration.help_text = "Migration that this Archive belongs to"

admin.site.register(MigrationArchive, MigrationArchiveAdmin)


class StorageQuotaAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'storage', 'workspace', 'quota_formatted_size',
                    'quota_formatted_used')
    list_filter = ('storage', 'workspace')
    fields = ('storage', 'workspace', 'quota_size', 'quota_formatted_used')
    readonly_fields = ('quota_formatted_used',)
    search_fields = ('workspace',)
admin.site.register(StorageQuota, StorageQuotaAdmin)


class StorageQuotaInline(admin.TabularInline):
    model = StorageQuota
    readonly_fields = ('quota_formatted_used',)
    fields = ('storage', 'quota_size', 'quota_formatted_used',)
    extra = 0


class GroupworkspaceAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'workspace',)
    fields = ('workspace', 'path', 'managers')
    search_fields = ('workspace',)
    filter_horizontal = ('managers',)
    inlines = [StorageQuotaInline]
admin.site.register(Groupworkspace, GroupworkspaceAdmin)
