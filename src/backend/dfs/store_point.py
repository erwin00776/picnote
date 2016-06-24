from base_point import BasePoint
from base_point import MachineType
import os
import ConfigParser
import sys
import json
import struct
sys.path.append("..")

from file_service import SimpleFileClient


class DummyStorePoint(BasePoint):
    def __init__(self):
        self.store_type = MachineType.STORE
        BasePoint.__init__(self)


class StorePoint(BasePoint):

    def __init__(self, root):
        BasePoint.__init__(self)
        self.store_type = MachineType.STORE
        self.store_level = 3
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root
        self.uid = ""

    def configure(self):
        config_path = os.path.join(self.root, '.simple.dfs.cfg')
        config_parser = ConfigParser.SafeConfigParser()
        config_parser.read(config_path)
        self.store_level = config_parser.read('base', 'store_level')

    def get_name(self, root):
        pass

    def store(self, peer_name, peer_ip, src, vals):
        rlevel = vals.get('store_level', 3)
        if self.store_level > rlevel:
            return None
        file_client = SimpleFileClient(peer_ip, 8072)
        base_name = os.path.basename(src)
        dst = os.path.join(self.root, base_name)
        print("store %s %s" % (src, dst))
        file_client.pull(src, dst)
        return dst

    def remove(self, src):
        print("remove %s" % src)
        dst = os.path.join(self.root, os.path.basename(src))
        if not os.path.exists(dst):
            return
        os.remove(dst)