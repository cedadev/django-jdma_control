from django.conf.urls import url
from jdma_control.views import *

urlpatterns = (
    url(r'^api/v1/user', UserView.as_view()),
    url(r'^api/v1/request', MigrationRequestView.as_view()),
    url(r'^api/v1/migration', MigrationView.as_view()),
    url(r'^api/v1/list_backends', list_backends, name="backends")
)
