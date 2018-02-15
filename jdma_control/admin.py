from django.contrib import admin
from jdma_control.models import *

# Register your models here.
class UserAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('name', 'email', 'notify')
    fields = ('name', 'email', 'notify')
    search_fields = ('name', 'email')
admin.site.register(User, UserAdmin)

class MigrationAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display  = ('pk','label', 'user', 'stage', 'workspace', 'storage', 'external_id', 'registered_date')
    list_filter   = ('stage','registered_date',)

    fields        = ('user','label','workspace','stage', 'storage', 'external_id',
                    'registered_date',
                    'unix_user_id','unix_group_id','unix_permission',
                    'original_path', 'failure_reason')
    readonly_fields = ('failure_reason',)

    search_fields = ('user','label','workspace','stage','external_id')
admin.site.register(Migration, MigrationAdmin)

class MigrationRequestAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'user', 'request_type', 'stage', 'date', 'migration', 'storage')
    list_filter = ('request_type', 'date',)

    fields = ('user', 'request_type', 'stage', 'date', 'migration', 'target_path', 'storage', 'credentials')
    search_fields = ('request_type',)
admin.site.register(MigrationRequest, MigrationRequestAdmin)

class MigrationFileAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'path', 'digest', 'migration')
    list_filter = ('migration',)
    fields = ('path', 'digest', 'migration')
    search_fields = ('path',)
admin.site.register(MigrationFile, MigrationFileAdmin)
