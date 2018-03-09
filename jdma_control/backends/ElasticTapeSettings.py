"""Settings for the ElasticTapeBackend"""

ET_QUOTA_URL = "http://et-monitor.fds.rl.ac.uk/et_user/ET_Holdings_Summary_XML.php"
ET_RSS_FILE = "http://et-monitor.fds.rl.ac.uk/et_rss/ET_RSS_AlertWatch_atom.php"
PAN_DU_EXE = "/usr/local/bin/pan_du"

# The place where files are downloaded to when verifying
VERIFY_DIR  = "/home/vagrant/JDMA_VERIFY"
# the place where the tar files are placed before being transferred to the storage
ARCHIVE_STAGING_DIR = "/home/vagrant/JDMA_STAGING"

# different put and get hosts
PUT_HOST = "jasmin_et-ingest.fds.rl.ac.uk"
GET_HOST = "jasmin_et-retrieval.fds.rl.ac.uk"
PORT = 7456
