import jdma_site.settings as settings

from django.db.models import Q
from jdma_control.models import MigrationRequest
from jdma_control.scripts.common import split_args
from jdma_control.scripts.unlock_requests import get_stage

def run(*args):
    arg_dict = split_args(args)
    if "stage" in arg_dict:
        stage = get_stage(arg_dict["stage"])
    else:
        raise Exception("stage argument not supplied")

    pr = MigrationRequest.objects.filter(
#         (Q(request_type=MigrationRequest.PUT)
#         | Q(request_type=MigrationRequest.MIGRATE)) &
         Q(locked=False)
         & Q(stage=stage)
    )

    for p in pr:
        print("Locking : {} ".format(p.pk))
        p.lock()
