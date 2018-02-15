"""Function to get the backends"""
from  jdma_control.backends import ElasticTapeBackend, FakeTapeBackend, ObjectStoreBackend

def get_backends():
    return [ElasticTapeBackend.ElasticTapeBackend,
            FakeTapeBackend.FakeTapeBackend,
            ObjectStoreBackend.ObjectStoreBackend]
