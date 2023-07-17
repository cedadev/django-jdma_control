import jdma_site.settings as settings

from django.db.models import Q
from jdma_control.models import MigrationRequest
from jdma_control.scripts.common import split_args

def get_stage(stage_string):
    """Convert stage_string to a MigrationRequest.stage number"""
    stage_choices = {
        'PUT_START'        : MigrationRequest.PUT_START,
        'PUT_BUILDING'     : MigrationRequest.PUT_BUILDING,
        'PUT_PENDING'      : MigrationRequest.PUT_PENDING,
        'PUT_PACKING'      : MigrationRequest.PUT_PACKING,
        'PUTTING'          : MigrationRequest.PUTTING,
        'VERIFY_PENDING'   : MigrationRequest.VERIFY_PENDING,
        'VERIFY_GETTING'   : MigrationRequest.VERIFY_GETTING,
        'VERIFYING'        : MigrationRequest.VERIFYING,
        'PUT_TIDY'         : MigrationRequest.PUT_TIDY,
        'PUT_COMPLETED'    : MigrationRequest.PUT_COMPLETED,

        'GET_START'        : MigrationRequest.GET_START,
        'GET_PENDING'      : MigrationRequest.GET_PENDING,
        'GETTING'          : MigrationRequest.GETTING,
        'GET_UNPACKING'    : MigrationRequest.GET_UNPACKING,
        'GET_RESTORE'      : MigrationRequest.GET_RESTORE,
        'GET_TIDY'         : MigrationRequest.GET_TIDY,
        'GET_COMPLETED'    : MigrationRequest.GET_COMPLETED,

        'DELETE_START'     : MigrationRequest.DELETE_START,
        'DELETE_PENDING'   : MigrationRequest.DELETE_PENDING,
        'DELETING'         : MigrationRequest.DELETING,
        'DELETE_TIDY'      : MigrationRequest.DELETE_TIDY,
        'DELETE_COMPLETED' : MigrationRequest.DELETE_COMPLETED,

        'FAILED'           : MigrationRequest.FAILED,
        'FAILED_COMPLETED' : MigrationRequest.FAILED_COMPLETED
    }
    return(stage_choices[stage_string])

def run(*args):
    arg_dict = split_args(args)
    if "stage" in arg_dict:
        stage = get_stage(arg_dict["stage"])
    else:
        raise Exception("stage argument not supplied")

    if "put_stuck" in arg_dict:
        put_stuck = True
    else:
        put_stuck = False

    print(arg_dict)
    print(put_stuck)
    pr = MigrationRequest.objects.filter(
         (Q(request_type=MigrationRequest.PUT)
         | Q(request_type=MigrationRequest.MIGRATE))
         & Q(locked=True)
         & Q(stage=stage)
    )

    for p in pr:
        # if stage is 'PUTTING' then unlock if there is an external id and put_stuck is true
        if stage == get_stage('PUTTING') and put_stuck:
            if p.migration.external_id is not None:
                p.unlock()
                print("Unlocking : {} ".format(p.pk))
        else:    
            p.unlock()
            print("Unlocking : {} ".format(p.pk))
