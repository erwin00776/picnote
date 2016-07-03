from base_point import BasePoint
from base_point import MachineType
from fs_scanner import FSScanner

import os
import shutil
import ConfigParser
import sys
import threading
import time
import json
import struct
sys.path.append("..")

from file_service import SimpleFileClient


class DummyStorePoint(BasePoint):
    def __init__(self):
        self.store_type = MachineType.STORE
        BasePoint.__init__(self)


class StorePoints(threading.Thread):
    def __init__(self, roots):
        self.is_shutdown = False
        self.roots = {}
        for root in roots:
            if root[0] == '/':
                self.roots[root] = MachineType.STORE
            else:
                self.roots[root] = MachineType.PROCESS
        threading.Thread.__init__(self, name="StorePoints")
        self.store_points = {}
        self.scan_interval = 30

    def add_root(self, peer_name, peer_ip):
        self.roots[peer_ip] = MachineType.PROCESS

    def run(self):
        while not self.is_shutdown:
            roots = {}
            for (root, root_type) in self.roots.items():
                if root_type == MachineType.STORE:
                    is_exists = os.path.exists(root)
                elif root_type == MachineType.PROCESS:
                    is_exists = False
                if is_exists:
                    roots[root] = root_type
            self.store_points = roots
            time.sleep(self.scan_interval)

    def store(self, peer_name, peer_ip, src, vals):     # [TODO] lock store_points
        for (point, point_type) in self.store_points.items():
            if point_type == MachineType.PROCESS:
                self.store(peer_name, peer_ip, src, vals)
            elif point_type == MachineType.STORE:
                self.local_store(src, vals)


class StorePoint(BasePoint):
    def __init__(self, root):
        BasePoint.__init__(self)
        self.store_type = MachineType.STORE
        self.store_level = 3
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root
        self.uid = ""
        self.thread_pool = []
        self.max_thread = 5

    def init(self):
        scanner = FSScanner(self.root)
        status, last_ts = scanner.scan_once()
        scanner.shutdown()
        return status, last_ts

    def configure(self):
        config_path = os.path.join(self.root, '.simple.dfs.cfg')
        config_parser = ConfigParser.SafeConfigParser()
        config_parser.read(config_path)
        self.store_level = config_parser.read('base', 'store_level')

    def get_name(self, root):
        pass

    def store(self, peer_name, peer_ip, src, vals):
        if self.store_type == MachineType.PROCESS:
            self.remote_store(peer_name, peer_ip, src, vals)
        elif self.store_type == MachineType.STORE:
            self.local_store(src, vals)

    def local_store(self, src, vals):
        base_name = os.path.basename(src)
        dst = os.path.join(self.root, base_name)
        shutil.copyfile(src, dst)

    def remote_store(self, peer_name, peer_ip, src, vals):
        while len(self.thread_pool) >= self.max_thread:
            alives = []
            for t in self.thread_pool:
                if t.isAlive():
                    alives.append(t)
            self.thread_pool = alives
        t = threading.Thread(target=self.__remote_store, args=(peer_name, peer_ip, src, vals))
        self.thread_pool.append(t)
        t.start()

    def __remote_store(self, peer_name, peer_ip, src, vals):
        rlevel = vals.get('store_level', 3)
        if self.store_level > rlevel:
            return None
        file_client = SimpleFileClient(peer_ip, 8073)
        base_name = os.path.basename(src)
        dst = os.path.join(self.root, base_name)
        print("store %s %s" % (src, dst))
        file_client.pull(src, dst)
        return dst

    def remove(self, src):
        if self.store_type == MachineType.STORE:
            self.local_remove(src)

    def local_remove(self, src):
        print("remove %s" % src)
        dst = os.path.join(self.root, os.path.basename(src))
        if not os.path.exists(dst):
            return
        os.remove(dst)

    def remote_remote(self, src):
        pass