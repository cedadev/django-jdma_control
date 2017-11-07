from jdma_control.backends import FakeTapeBackend#, ElasticTapeBackend , ObjectStoreBackend
import jdma_site.settings as settings
settings.JDMA_BACKEND_OBJECT = eval(settings.JDMA_BACKEND+"."+settings.JDMA_BACKEND+"()")
