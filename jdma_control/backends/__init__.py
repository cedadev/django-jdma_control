"""Function to get the backends"""
from jdma_control.backends import ElasticTapeBackend
from jdma_control.backends import FakeElasticTapeBackend
from jdma_control.backends import ObjectStoreBackend

def get_backends():
    return [ElasticTapeBackend.ElasticTapeBackend,
            FakeElasticTapeBackend.FakeElasticTapeBackend,
            ObjectStoreBackend.ObjectStoreBackend]
