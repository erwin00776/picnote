__author__ = 'erwin'

import sys

CUR_SYSTEM = None
SYSPATH_PREFIX = None
if sys.platform == 'darwin':
    CUR_SYSTEM = 'darwin'
    SYSPATH_PREFIX = '/Users/erwin'
else:
    CUR_SYSTEM = 'linux'
    SYSPATH_PREFIX = '/home/erwin'