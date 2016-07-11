import ConfigParser
import json
import os
import pickle
import shutil
import sys
import threading
import time

from base_point import BasePoint
from base_point import MachineType
from fs_scanner import FSScanner
from src.backend.utils.dfs_log import LOG
from src.backend.utils.superior_thread import SuperiorThread

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


class StorePoints(SuperiorThread):
    def __init__(self, roots, redis_cli):
        self.is_shutdown = False
        self.redis_cli = redis_cli
        self.roots = roots
        self.cur_roots = []
        # threading.Thread.__init__(self, name="StorePoints")
        SuperiorThread.__init__(self, name="StorePoints")
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
        self.points_lock.acquire()
        self.cur_roots.append(root)
        store_point = StorePoint(root, store_type=MachineType.STORE, redis_cli=self.redis_cli)

        last_ts, store_meta = store_point.self_check()
        self.last_changed_ts = self.last_changed_ts if self.last_changed_ts > last_ts else last_ts
        for r, p in self.store_points.items():                  # balance between all local store.
            if r == root:
                continue
            p_last_ts, p_meta = p.get_metas()
            lefts, rights = meta_diff(store_meta, p_meta)
            for md5id, val in lefts.items():
                try:
                    p.store(None, None, val['md5id'], val)
                except Exception as e:
                    LOG.info("%s -- %s, %s" % (root, r, e.message))
            for md5id, val in rights.items():
                try:
                    store_point.store(None, None, val['md5id'], val)
                except Exception as e:
                    LOG.info("%s -- %s, %s" % (r, root, e.message))
        self.store_points[root] = store_point
        self.points_lock.release()
        LOG.info("StorePoint[%s] plugged." % root)

    def remove_local(self, root):
        self.cur_roots.remove(root)
        del self.store_points[root]
        LOG.info("StorePoint[%s] unplugged." % root)

    def refresh_roots(self):
        roots, added, deleted = [], [], []
        cur = set(self.cur_roots)
        for root in self.roots:
            is_exists = os.path.exists(root)
            if is_exists:
                roots.append(root)
            if is_exists and root not in cur:           # store added
                added.append(root)
            elif not is_exists and root in cur:         # store removed
                deleted.append(root)

        for root in added:
            self.add_local(root)
        for root in deleted:
            self.remove_local(root)
        self.cur_roots = roots

    def wait4init(self):
        if not self.roots and len(self.roots) == 0:
            return True
        cur_roots = []
        for root in self.roots:
            if os.path.exists(root):
                cur_roots.append(root)
        if len(cur_roots) > len(self.store_points):
            return False
        for root, point in self.store_points.items():
            if not point or not point.first_inited():
                return False
        return True

    def run(self):
        self.refresh_roots()
        self.run_first = True
        while not self.is_shutdown:
            self.refresh_roots()
            time.sleep(self.scan_interval)

    def store(self, peer_name, peer_ip, md5id, val):     # [TODO] lock store_points
        if not val['src'] or len(val['src']) < 1:
            raise ValueError("store error: %s" % str(val))
        self.points_lock.acquire()
        for point_name, point in self.store_points.items():
            point.store(peer_name, peer_ip, md5id, val)
        self.points_lock.release()

    def remove(self, val):
        pass


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
        self.inited = False
        LOG.info("** store point started %s" % self.root)

    def first_inited(self):
        return self.inited

    def get_metas(self):
        return self.store_last_ts, self.store_meta

    def self_check(self):
        last_ts, meta = self.load_seq_pickle()
        file_list = self.scan_local()
        file_list = set(file_list)
        self.store_meta = {}
        self.store_last_ts = 0
        for md5id, val in meta.items():
            if val['src'] in file_list:
                self.store_meta[md5id] = val
                if val['mtime'] > self.store_last_ts:
                    self.store_last_ts = val['mtime']
        self.inited = True
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
            self.seq_file.close()
            self.seq_file = None
        LOG.debug("load_seq pickle done")
        return last_ts, meta

    def scan_local(self):                            # scan meta from disk
        # TODO why core?
        """ return file list of current disk """
        scanner = FSScanner(to_monitor=self.root, need_thread=False)
        cur_meta, cur_ts = scanner.scan_once(skip_hidden=True)
        scanner.shutdown()
        file_list = [val['src'] for _x, val in cur_meta.items()]
        return file_list

    def default_configure(self):
        self.store_level = 3

    def configure(self):
        config_path = os.path.join(self.root, '.simple.dfs.cfg')
        if not os.path.exists(config_path):
            self.default_configure()
            return
        try:
            config_parser = ConfigParser.SafeConfigParser()
            config_parser.read(config_path)
            self.store_level = config_parser.read('base', 'store_level')
        except:
            # ignore when read error.
            self.default_configure()
            return

    def update_pickle(self):
        try:
            if self.pickle_file is None or self.pickle_file.closed:
                self.pickle_file = None
                self.pickle_file = open(self.pickle_path, 'w')
            pickle.dump(self.store_meta, self.pickle_file)
            pickle.dump(self.store_last_ts, self.pickle_file)
            self.pickle_file.close()

            self.seq_file.close()
            os.remove(self.seq_path)
            self.seq_file = None

            return True
        except IOError as e:
            LOG.debug("error while pickle %s" % e.message)
            return False

    def update_seq(self, op, src, val):
        if self.seq_file is None:
            self.seq_file = open(self.seq_path, 'w')

        val['src'] = val['dst']                     # reset val for local meta
        val['dst'] = ''
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
        src = val['src']
        base_name = os.path.basename(src)
        dst = os.path.join(self.root, base_name)
        val_copy = val.copy()
        val_copy['dst'] = dst
        val['dst'] = dst
        if peer_ip is not None:
            self.__store(peer_name, peer_ip, md5id, val_copy, fn=self.__store_from_remote)
        else:
            self.__store(peer_name, peer_ip, md5id, val_copy, fn=self.__store_from_local)
        self.update_seq('A', md5id, val)

    def __store(self, peer_name, peer_ip, md5id, val, fn):
        while len(self.thread_pool) >= self.max_thread:
            alives = []
            for t in self.thread_pool:
                if t.isAlive():
                    alives.append(t)
            self.thread_pool = alives
        if val['src'] == val['dst'] and not peer_ip:
            LOG.error("bad src, dst %s" % self.root)
            raise Exception("src and dst is same file, %s" % str(val))
        t = threading.Thread(target=fn, args=(peer_name, peer_ip, md5id, val))
        self.thread_pool.append(t)
        t.start()

    def __store_from_local(self, peer_name, peer_ip, md5id, val, try_max=5):
        assert (val['src'] and val['dst'])
        try:
            try_cur = 0
            while try_cur < try_max:
                shutil.copyfile(val['src'], val['dst'])
                if os.path.exists(val['dst']):
                    st = os.stat(val['dst'])
                    if val['size'] == st.st_size:
                        return True
                try_cur += 1
        except IOError as e:
            LOG.error("copy local file error: %s, %s" % (str(val), e.message))
        return False

    def __store_from_remote(self, peer_name, peer_ip, md5id, val, try_max=5):
        assert (val['src'] and val['dst'])
        try:
            rlevel = val.get('store_level', 3)
            if self.store_level > rlevel:
                return False
            file_client = SimpleFileClient(peer_ip, 8073)
            try_cur = 0
            while try_cur < try_max:
                file_client.pull(val['src'], val['dst'], val['size'])
                if os.path.exists(val['dst']):
                    st = os.stat(val['dst'])
                    if val['size'] == st.st_size:
                        return True
                try_cur += 1
        except ValueError as e:
            LOG.error("store from remote: %s %s error: %s" % (peer_ip, str(val), e.message))
        except IOError as e:
            LOG.error("pull file error: %s" % str(val))
        return False

    def remove(self, src, val):
        LOG.debug("removed %s" % src)
        md5id = val['md5id']
        dst = self.store_meta[md5id]['dst']
        del self.store_meta[md5id]
        dst = os.path.join(self.root, os.path.basename(src))
        if not os.path.exists(dst):
            return
        os.remove(dst)
        self.update_seq('D', src, val)
