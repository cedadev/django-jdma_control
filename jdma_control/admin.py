from django.contrib import admin
from jdma_control.models import *

# Register your models here.
class UserAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display = ('name', 'email', 'notify')
    fields = ('name', 'email', 'notify')
    search_fields = ('name', 'email')
admin.site.register(User, UserAdmin)

class MigrationRequestAdmin(admin.ModelAdmin):
    save_on_top = True
    list_display  = ('pk','label','user','workspace','request_type','stage','et_id',
                    'registered_date')
    list_filter   = ('request_type','stage')

    fields        = ('user','label','workspace','request_type','stage','et_id','tags',
                    'registered_date','unix_user_id','unix_group_id','unix_permission',
                    'original_path')

    search_fields = ('user','label','workspace','request_type','stage','et_id')
admin.site.register(MigrationRequest, MigrationRequestAdmin)
