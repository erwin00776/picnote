from base_point import BasePoint
from base_point import MachineType
from fs_scanner import FSScanner

import os
import pickle
import threading
import shutil
import ConfigParser
import sys
import threading
import time
import json
import struct
sys.path.append("..")

from file_service import SimpleFileClient


def meta_diff(m1, m2):
    """
    :param m1: compare hash
    :param m2: base meta hash
    :return: added/deleted
    """
    added, deleted = {}, m2.copy()
    for md5id, val in m1.items():
        if md5id not in m2:
            added[md5id] = val
        else:
            del deleted[md5id]
    return added, deleted


class StorePoints(threading.Thread):
    def __init__(self, roots, redis_cli):
        self.is_shutdown = False
        self.redis_cli = redis_cli
        self.roots = {}
        self.cur_roots = {}
        for root in roots:
            if root[0] == '/':
                self.roots[root] = MachineType.STORE
            else:
                self.roots[root] = MachineType.PROCESS
        threading.Thread.__init__(self, name="StorePoints")
        self.store_points = {}
        self.scan_interval = 30
        self.last_changed_ts = 0
        self.store_metas = {}
        self.store_points = {}
        self.points_lock = threading.Lock()
        self.run_first = False

    def get_metas(self):
        while not self.run_first:
            time.sleep(1)
        metas = {}
        for root, point in self.store_points.items():
            sub_ts, sub_meta = point.get_metas()
            self.last_changed_ts = self.last_changed_ts if self.last_changed_ts > sub_ts else sub_ts
            metas.update(sub_meta)
        return self.last_changed_ts, metas

    def add_local(self, root):
        self.cur_roots[root] = MachineType.STORE
        store_point = StorePoint(root, store_type=MachineType.STORE, redis_cli=self.redis_cli)

        last_ts, store_meta = store_point.self_check()
        self.last_changed_ts = self.last_changed_ts if self.last_changed_ts > last_ts else last_ts
        for r, p in self.store_points.items():                  # balance between all local store.
            added, deleted = meta_diff(store_meta, p.get_metas())
            for md5id, val in added.items():
                store_point.store(None, None, val['dst'], val)
            for md5id, val in deleted.items():
                p.store(None, None, val['dst'], val)
        self.store_points[root] = store_point

    def remove_local(self, root):
        del self.cur_roots[root]
        del self.store_points[root]

    def refresh_local(self):
        roots = {}
        added, deleted = {}, {}
        cur = set(self.cur_roots.keys())
        for (root, root_type) in self.roots.items():
            is_exists = True
            if root_type == MachineType.STORE:
                is_exists = os.path.exists(root)
            if is_exists:
                roots[root] = MachineType.STORE
            if is_exists and root not in cur:           # store added
                added[root] = MachineType.STORE
            elif not is_exists and root in cur:         # store removed
                deleted[root] = MachineType.STORE

        for root, root_type in added.items():
            self.add_local(root)
        for root, root_type in deleted.items():
            self.remove_local(root)
        self.cur_roots = roots

    def run(self):
        self.refresh_local()
        self.run_first = True
        while not self.is_shutdown:
            self.refresh_local()
            time.sleep(self.scan_interval)

    def store(self, peer_name, peer_ip, md5id, val):     # [TODO] lock store_points
        self.points_lock.acquire()
        for point_name, point in self.store_points.items():
            point.store(peer_name, peer_ip, md5id, val)
        self.points_lock.release()


class StorePoint(BasePoint):
    def __init__(self, root, store_type, redis_cli):
        BasePoint.__init__(self)
        self.store_type = store_type
        self.store_level = 3
        if not os.path.exists(root):
            os.mkdir(root)
        self.root = root
        self.pickle_path = os.path.join(self.root, ".meta.pickle")
        self.seq_path = os.path.join(self.root, ".meta.sequences")
        self.seq_num = 0
        self.pickle_file = None
        self.seq_file = None
        self.last_pickle_ts = int(time.time())
        self.thread_pool = []
        self.max_thread = 5
        self.is_shutdown = False
        self.store_meta = {}
        self.store_last_ts = 0
        self.redis_cli = redis_cli

    def get_metas(self):
        return self.store_last_ts, self.store_meta

    def self_check(self):
        last_ts, meta = self.load_seq_pickle()
        file_list = self.scan_local()
        file_list = set(file_list)
        self.store_meta = {}
        self.store_last_ts = 0
        for md5id, val in meta.items():
            if val['dst'] in file_list:
                self.store_meta[md5id] = val
                if val['mtime'] > self.store_last_ts:
                    self.store_last_ts = val['mtime']
        return self.store_last_ts, self.store_meta

    def load_seq_pickle(self):
        meta = {}
        last_ts = 0
        if os.path.exists(self.pickle_path):
            self.pickle_file = open(self.pickle_path, 'r')
            meta = pickle.load(self.pickle_file)
            last_ts = pickle.load(self.pickle_file)
            self.pickle_file.close()
            self.pickle_file = None
        if os.path.exists(self.seq_path):
            self.seq_file = open(self.seq_path, 'r')
            for line in self.seq_file.readlines():
                line = line.strip()
                op, md5id, val_str = line.split('$')
                if op == 'D':
                    del meta[md5id]
                elif op == 'A':
                    val = json.loads(val_str)
                    if last_ts < val['mtime']:
                        last_ts = val['mtime']
                    meta[md5id] = val
        return last_ts, meta

    def scan_local(self):                            # scan meta from disk
        """ return file list of current disk """
        scanner = FSScanner(self.root)
        cur_meta, cur_ts = scanner.scan_once()
        scanner.shutdown()
        file_list = [val['src'] for _x, val in cur_meta.items()]
        return file_list

    def configure(self):
        config_path = os.path.join(self.root, '.simple.dfs.cfg')
        if not os.path.exists(config_path):
            return
        try:
            config_parser = ConfigParser.SafeConfigParser()
            config_parser.read(config_path)
            self.store_level = config_parser.read('base', 'store_level')
        except:
            # ignore when read error.
            return

    def update_pickle(self):
        try:
            if self.pickle_file is None:
                self.pickle_file = open(self.pickle_path, 'w')
            pickle.dump(self.store_meta, self.pickle_file)
            pickle.dump(self.store_last_ts, self.pickle_file)
            self.pickle_file.close()

            self.seq_file.close()
            os.remove(self.seq_path)
            self.seq_file = None

            return True
        except IOError as e:
            print("error while pickle %s" % e.message)
            return False

    def update_seq(self, op, src, val):
        if self.seq_file is None:
            self.seq_file = open(self.seq_path, 'w')

        self.store_meta[val['md5id']] = val

        self.seq_file.write("%s$%s$%s\n" % (op, val['md5id'], json.dumps(val)))
        self.seq_file.flush()
        self.seq_num += 1
        cur_ts = int(time.time())
        if self.seq_num > 20 or cur_ts - self.last_pickle_ts > 30:
            self.update_pickle()
            self.seq_num = 0
            self.last_pickle_ts = cur_ts

    def store(self, peer_name, peer_ip, md5id, val): # store one file
        if val['mtime'] > self.store_last_ts:
            self.store_last_ts = val['mtime']
        if peer_ip is not None:
            self.__store(peer_name, peer_ip, md5id, val, fn=self.__store_from_remote)
        else:
            self.__store(peer_name, peer_ip, md5id, val, fn=self.__store_from_local)
        self.update_seq('A', md5id, val)

    def __store(self, peer_name, peer_ip, md5id, val, fn):
        while len(self.thread_pool) >= self.max_thread:
            alives = []
            for t in self.thread_pool:
                if t.isAlive():
                    alives.append(t)
            self.thread_pool = alives
        base_name = os.path.basename(md5id)
        dst = os.path.join(self.root, base_name)
        val['dst'] = dst

        t = threading.Thread(target=fn, args=(peer_name, peer_ip, md5id, val))
        self.thread_pool.append(t)
        t.start()

    def __store_from_local(self, peer_name, peer_ip, md5id, val):
        md5id = val['src']
        dst = val['dst']
        shutil.copyfile(md5id, dst)

    def __store_from_remote(self, peer_name, peer_ip, md5id, val):
        try:
            print("store from remote: ", peer_ip, md5id, val)
            rlevel = val.get('store_level', 3)
            if self.store_level > rlevel:
                return None
            file_client = SimpleFileClient(peer_ip, 8073)
            src = val['src']
            dst = val['dst']
            file_client.pull(src, dst)
        except:
            print("store from remote: error: ", peer_ip, src, dst)
        return dst

    def remove(self, src, val):
        print("removed %s" % src)
        md5id = val['md5id']
        dst = self.store_meta[md5id]['dst']
        del self.store_meta[md5id]
        dst = os.path.join(self.root, os.path.basename(src))
        if not os.path.exists(dst):
            return
        os.remove(dst)
        self.update_seq('D', src, val)
