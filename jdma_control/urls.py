from django.urls import re_path
from jdma_control.views import *

urlpatterns = (
    re_path(r'^api/v1/user', UserView.as_view()),
    re_path(r'^api/v1/request', MigrationRequestView.as_view()),
    re_path(r'^api/v1/migration/', MigrationView.as_view()),
    re_path(r'^api/v1/archive/', MigrationArchiveView.as_view()),
    re_path(r'^api/v1/file/', MigrationFileView.as_view()),
    re_path(r'^api/v1/list_backends/', list_backends, name="backends")
)
