"""Function to get the backends"""
from jdma_control.backends import ElasticTapeBackend
# from jdma_control.backends import ObjectStoreBackend
# from jdma_control.backends import FTPBackend

def get_backends():
    return [ElasticTapeBackend.ElasticTapeBackend,]
            # ObjectStoreBackend.ObjectStoreBackend,
            # FTPBackend.FTPBackend]

def get_backend_ids():
    return [x.get_id(None) for x in get_backends()]

def get_backend_from_id(id):
    index = get_backend_ids().index(id)
    return get_backends()[index]
