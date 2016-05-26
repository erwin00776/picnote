__author__ = 'erwin'

import sys
import logging

############################ Common Configure ############################
CUR_SYSTEM = None
SYSPATH_PREFIX = None
if sys.platform == 'darwin':
    CUR_SYSTEM = 'darwin'
    SYSPATH_PREFIX = '/Users/erwin'
else:
    CUR_SYSTEM = 'linux'
    SYSPATH_PREFIX = '/home/erwin'

THUMBNAILS_PATH = SYSPATH_PREFIX + "/data/thumbnails"
LAST_SCAN_FILENAME = '.picnote.last_scan.timestamp'

_DEFAULT_LOG_FORMAT = '%(levelname)s-%(asctime)s-%(name)s-%(message)s'
LOGGER = logging.getLogger(__name__)
def _configure_logging():
    LOGGER.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
    sh.setFormatter(formatter)
    LOGGER.addHandler(sh)
_configure_logging()




############################ Generating Words ############################
STOPWORD_PATH = SYSPATH_PREFIX + "/data/stopwords"
NEURAL_TALK = "/home/erwin/git-repos/neuraltalk2"




############################ Redis Configure  ############################
REDIS_ADDR = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0