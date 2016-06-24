from base_point import BasePoint
import sys
sys.path.append("..")
from fs_scanner import FSScanner


class SourcePoint(BasePoint):
    def __init__(self, roots):
        BasePoint.__init__(self)
        self.fsscanner = FSScanner(roots[0], scan_interval=15)
        # self.fsscanner = FSScanner("/home/pi/tmp", scan_interval=15)
        self.fsscanner.start()

    def get_files_meta(self):
        return self.fsscanner.get_files_meta()
