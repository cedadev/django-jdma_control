# App specific settings for jdma_control

LOG_PATH = "/var/log/jdma"
FILE_LIST_PATH = "/jdma_file_lists/"
TESTING = True

if TESTING:
    ET_RSS_FILE = "/jdma_rss_feed/test_feed.xml"
    # directory for FAKE_ET
    FAKE_ET_DIR = "/home/vagrant/fake_et"
    # directory to pull data back for verification
    VERIFY_DIR = "/home/vagrant/verify_dir"
else:
    # RSS feeds - URL for production, FILE for TESTING version
    ET_RSS_URL = "http://et-monitor.fds.rl.ac.uk/et_rss/ET_RSS_AlertWatch_atom.php"
