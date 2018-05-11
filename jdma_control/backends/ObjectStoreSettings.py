"""Settings for the Object Store backend"""

# The place where files are downloaded to when verifying
VERIFY_DIR  = "/home/vagrant/JDMA_VERIFY"
# the place where the tar files are placed before being transferred to the storage
ARCHIVE_STAGING_DIR = "/home/vagrant/JDMA_STAGING"

S3_ENDPOINT = "http://192.168.51.30:9000"
OBJECT_SIZE = 1000*10**6
