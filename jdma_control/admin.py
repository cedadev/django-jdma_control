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
    list_display  = ('pk','label','user','workspace','stage','et_id', 'registered_date')
    list_filter   = ('stage','registered_date',)

    fields        = ('user','label','workspace','stage','et_id', 'tags',
                    'registered_date','unix_user_id','unix_group_id','unix_permission',
                    'original_path')

    search_fields = ('user','label','workspace','stage','et_id')
admin.site.register(Migration, MigrationAdmin)

class MigrationRequestAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('pk', 'user', 'request_type','date','migration')
    list_filter = ('request_type', 'date',)

    fields = ('user', 'request_type', 'date', 'migration', 'target_path')
    search_fields = ('request_type',)
admin.site.register(MigrationRequest, MigrationRequestAdmin)