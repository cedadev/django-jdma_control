"""Function to get the backends"""
from jdma_control.backends import ElasticTapeBackend
#from jdma_control.backends import FakeElasticTapeBackend
from jdma_control.backends import ObjectStoreBackend

def get_backends():
    return [ElasticTapeBackend.ElasticTapeBackend,
            #FakeElasticTapeBackend.FakeElasticTapeBackend,
            ObjectStoreBackend.ObjectStoreBackend]

def get_backend_ids():
    return [x.get_id(None) for x in get_backends()]

def get_backend_from_id(id):
    index = get_backend_ids().index(id)
    return get_backends()[index]
