import jdma_site.settings as settings

from django.db.models import Q
from jdma_control.models import MigrationRequest


def run(*args):
    pr = MigrationRequest.objects.filter(
         (Q(request_type=MigrationRequest.PUT)
         | Q(request_type=MigrationRequest.MIGRATE))
         & Q(locked=True)
         & Q(stage=MigrationRequest.PUTTING)
    )

    for p in pr:
        p.unlock()
