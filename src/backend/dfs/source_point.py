from base_point import BasePoint
import sys
sys.path.append("..")
from dfs_client.fs_scanner import FSScanner


class SourcePoint(BasePoint):
    def __init__(self):
        BasePoint.__init__(self)
        self.fsscanner = FSScanner("/home/erwin/cbuild", scan_interval=15)

    def get_files_meta(self):
        return self.fsscanner.get_files_meta()
