"""Function to get the backends"""
from jdma_control.backends import ElasticTapeBackend, FakeElasticTapeBackend, ObjectStoreBackend

def get_backends():
    return [ElasticTapeBackend.ElasticTapeBackend,
            FakeElasticTapeBackend.FakeElasticTapeBackend,
            ObjectStoreBackend.ObjectStoreBackend]
