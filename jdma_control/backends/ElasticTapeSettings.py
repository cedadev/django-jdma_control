"""Settings for the ElasticTapeBackend"""

ET_QUOTA_URL = "http://et-monitor.fds.rl.ac.uk/et_user/ET_Holdings_Summary.php"
ET_ROLE_URL  = "http://et-monitor.fds.rl.ac.uk/et_admin/ET_Role_List.php"
ET_HOLDINGS_URL = "http://et-monitor.fds.rl.ac.uk/et_user/ET_Holdings_Summary_XML.php"

# The place where files are downloaded to when verifying
VERIFY_DIR  = "/home/vagrant/JDMA_VERIFY"
# the place where the tar files are placed before being transferred to the storage
ARCHIVE_STAGING_DIR = "/home/vagrant/JDMA_STAGING"

# different put and get hosts
PUT_HOST = "jasmin_et-ingest.fds.rl.ac.uk"
GET_HOST = "jasmin_et-retrieval.fds.rl.ac.uk"
PORT = 7456
OBJECT_SIZE = 2 * 1000**3
