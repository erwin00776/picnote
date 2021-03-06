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
from src.backend.handlers.filetype_processors import FileTypeHelper
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
        SuperiorThread.__init__(self, name="StorePoints")
        self.store_points = {}
        self.scan_interval = 30
        self.last_changed_ts = 0
        self.store_metas = {}
        self.store_points = {}
        self.points_lock = threading.Lock()
        self.run_first = False

    def get_metas(self):
        """
        get all metas in this machine.
        :return: {md5id: {meta}}
        """
        while not self.run_first:
            time.sleep(1)
        metas = {}
        for root, point in self.store_points.items():
            sub_ts, sub_meta = point.get_metas()
            self.last_changed_ts = self.last_changed_ts if self.last_changed_ts > sub_ts else sub_ts
            metas.update(sub_meta)
        return self.last_changed_ts, metas

    def add_local(self, root):
        """ add local store point.
        :param root:
        :return: None
        """
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
                    LOG.error("%s -- %s, %s" % (r, root, e.message))
        self.store_points[root] = store_point
        self.points_lock.release()
        LOG.info("StorePoint[%s] plugged." % root)

    def remove_local(self, root):
        self.cur_roots.remove(root)
        del self.store_points[root]
        LOG.info("StorePoint[%s] unplugged." % root)

    def refresh_roots(self, need_balance=False):
        """ refresh local roots whether exists?
        :return: None
        """
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

        if not need_balance:
            return None
        merged_metas = {}
        for root, metas in self.store_metas:
            for md5id, meta in metas:
                merged_metas[md5id] = meta
        for root, metas in self.store_metas:
            if len(metas) < len(merged_metas):
                for md5id, meta in merged_metas.items():
                    if md5id not in metas:
                        # add missing file.
                        store_point = self.store_points.get(root)
                        store_point.store(None, None, md5id=md5id, val=meta)

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
        self.refresh_roots(need_balance=True)
        last_store_balance_ts = time.time()
        self.run_first = True
        while not self.is_shutdown:
            cur_ts = time.time()
            if cur_ts - last_store_balance_ts > 15 * 60:
                self.refresh_roots(need_balance=True)
                last_store_balance_ts = time.time()
            else:
                self.refresh_roots(need_balance=False)
            time.sleep(self.scan_interval)

    def store(self, peer_name, peer_ip, md5id, val):     # [TODO] lock store_points
        if not val['src'] or len(val['src']) < 1:
            raise ValueError("store error: %s" % str(val))
        self.points_lock.acquire()
        for point_name, point in self.store_points.items():
            val_copy = val.copy()
            try:
                point.store(peer_name, peer_ip, md5id, val_copy)
            except IOError as e:
                LOG.error("store error: %s, ip:%s, dst:%s" % (e.message, md5id, val['dst']))
        self.points_lock.release()

    def remove(self, val):
        pass


class StorePoint(BasePoint):
    default_usage_percent = 0.7

    def __init__(self, root, store_type, redis_cli, max_capacity_size=-1):
        BasePoint.__init__(self)
        self.file_type = FileTypeHelper(store_path=root, redis_cli=redis_cli)
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
        self.seq_update_lock = threading.RLock()
        self.last_pickle_ts = int(time.time())
        self.thread_pool = []
        self.max_thread = 5
        self.is_shutdown = False
        self.store_meta = {}
        self.store_last_ts = 0
        self.redis_cli = redis_cli
        self.inited = False

        if max_capacity_size <= 0:
            max_capacity_size = self.get_disk_total_space() * self.default_usage_percent
        self.total_files_size = 0
        self.max_capacity_size = max_capacity_size

        LOG.info("** store point started %s" % self.root)

    def get_disk_total_space(self):
        """return in bytes
        """
        f = os.statvfs(self.root)
        return f.f_frsize * f.f_blocks

    def first_inited(self):
        return self.inited

    def get_metas(self):
        return self.store_last_ts, self.store_meta

    def self_check(self):
        """ load meta info from store dir,
                and start self check.
        :return:
        """
        last_ts, meta = self.load_seq_pickle()
        file_list = self.scan_local()
        self.store_meta = {}
        self.store_last_ts = 0
        self.total_files_size = 0
        for md5id, val in meta.items():
            if val['src'] in file_list:
                meta_size, disc_size = val['size'], file_list[val['src']]
                if meta_size > disc_size:
                    # delete bad file
                    LOG.warn("self check: deleted bad %s [%d, %d]" % (val['src'], meta_size, disc_size))
                    os.remove(val['src'])
                    continue
                self.store_meta[md5id] = val
                self.total_files_size += disc_size
                if val['mtime'] > self.store_last_ts:
                    self.store_last_ts = val['mtime']
        self.inited = True
        return self.store_last_ts, self.store_meta

    def load_seq_pickle(self):
        meta = {}
        last_ts = 0
        try:
            if os.path.exists(self.pickle_path):
                self.pickle_file = open(self.pickle_path, 'r')
                meta = pickle.load(self.pickle_file)
                last_ts = pickle.load(self.pickle_file)
                self.pickle_file.close()
                self.pickle_file = None
            if os.path.exists(self.seq_path):
                self.seq_file = open(self.seq_path, 'r')
                for line in self.seq_file.readlines():
                    line = line.strip('\n')
                    try:
                        op, md5id, val_str = line.split('$')
                        if op == 'D':
                            del meta[md5id]
                        elif op == 'A':
                            val = json.loads(val_str)
                            if last_ts < val['mtime']:
                                last_ts = val['mtime']
                            meta[md5id] = val
                    except:
                        continue
                self.seq_file.close()
                self.seq_file = None
        except IOError as e:
            LOG.error("ioerror while loading meta and seq file: " + e.message + " " +
                      self.pickle_path + " " + self.seq_path)
        except TypeError as e:
            LOG.error("type error while loading meta and seq file: " + e.message + " " +
                      self.pickle_path + " " + self.seq_path)
        LOG.debug("load_seq pickle done")
        return last_ts, meta

    def scan_local(self):
        """
        return file list of current disk
        :return {src: size}
        """
        scanner = FSScanner(to_monitor=self.root, need_thread=False)
        cur_meta, cur_ts = scanner.scan_once(skip_hidden=True)
        scanner.shutdown()
        file_list = {val['src']: val['size'] for _x, val in cur_meta.items()}
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
            # in case of core dump while pickle dumping
            # 1) first, write meta into tmp file;
            pickle_path_tmp = self.pickle_path + ".tmp"
            if self.pickle_file is None or self.pickle_file.closed:
                self.pickle_file = None
                self.pickle_file = open(pickle_path_tmp, 'w')
            pickle.dump(self.store_meta, self.pickle_file)
            pickle.dump(self.store_last_ts, self.pickle_file)
            self.pickle_file.close()

            # 2) second, rename file name;
            os.rename(pickle_path_tmp, self.pickle_path)

            # 3) third, remove sequence file.
            self.seq_file.close()
            os.remove(self.seq_path)
            self.seq_file = None

            return True
        except IOError as e:
            LOG.debug("error while pickle %s" % e.message)
            return False

    def update_seq(self, op, src, val):
        new_total_file_size = val['size'] + self.total_files_size
        if new_total_file_size > self.max_capacity_size:
            LOG.error("store dir is fulled! max_capacity: %0.2f(MB), current: %0.2f(MB)" %
                      (self.max_capacity_size, self.total_files_size))
            return
        with self.seq_update_lock:
            if self.seq_file is None:
                self.seq_file = open(self.seq_path, 'w')

            val['src'] = val['dst']                     # reset val for local meta
            val['dst'] = ''
            self.store_meta[val['md5id']] = val

            self.total_files_size = new_total_file_size

            self.seq_file.write("%s$%s$%s\n" % (op, val['md5id'], json.dumps(val)))
            self.seq_file.flush()
            self.seq_num += 1
            cur_ts = int(time.time())
            if self.seq_num > 20 or cur_ts - self.last_pickle_ts > 30:
                self.update_pickle()
                self.seq_num = 0
                self.last_pickle_ts = cur_ts

    def store(self, peer_name, peer_ip, md5id, val):
        """ store one file
        :param peer_name:
        :param peer_ip:
        :param md5id:
        :param val:
        :return:
        """
        if val['mtime'] > self.store_last_ts:
            self.store_last_ts = val['mtime']
        src = val['src']
        if not peer_ip and not os.path.exists(src):
            LOG.error("store error: file not exists: %s " % src)
            return

        # get dst.
        md5id = val['md5id']
        tmp_dst = self.file_type.pre_process(md5id, src)

        val_copy = val.copy()
        val_copy['dst'] = tmp_dst
        val['dst'] = tmp_dst
        if peer_ip is not None:
            self.__store(peer_name, peer_ip, md5id, val_copy, fn=self.__store_from_remote)
        else:
            self.__store(peer_name, peer_ip, md5id, val_copy, fn=self.__store_from_local)

    def __store(self, peer_name, peer_ip, md5id, val, fn):
        while len(self.thread_pool) >= self.max_thread:
            alives = []
            for t in self.thread_pool:
                if t.isAlive():
                    alives.append(t)
            self.thread_pool = alives
            time.sleep(1)
        if val['src'] == val['dst'] and not peer_ip:
            LOG.error("bad src, dst %s" % self.root)
            raise Exception("src and dst is same file, %s" % str(val))
        t = threading.Thread(target=fn, args=(peer_name, peer_ip, md5id, val))
        self.thread_pool.append(t)
        t.start()

    def __post_store(self, md5id, tmp_dst, src, expect_size, metas):
        if not os.path.exists(tmp_dst):
            LOG.warn("post store: tmp dst file does not exists.")
            return False
        try:
            st = os.stat(tmp_dst)
            if st.st_size != expect_size and expect_size > 0:
                LOG.warn("post store: tmp dst file size diff: %d %d." % (st.st_size, expect_size))
                return False
            dst = self.file_type.process(md5id, tmp_dst)
            shutil.move(tmp_dst, dst)
            metas['dst'] = dst
            self.update_seq('A', md5id, metas)
        except IOError as e:
            LOG.error("post store: ioerror, %s" % e.message)
            return False

    def __store_from_local(self, peer_name, peer_ip, md5id, val, try_max=5):
        """ copy file from local disk to local store point
        :param peer_name:   empty
        :param peer_ip:     empty
        :param md5id:       md5id
        :param val:         meta info
        :param try_max: int, max try count
        :return:
        """
        assert (val['src'] and val['dst'])
        try:
            try_cur = 0
            src = val['src']
            tmp_dst = val['dst']
            while try_cur < try_max:
                shutil.copyfile(src, tmp_dst)
                if os.path.exists(tmp_dst):
                    st = os.stat(tmp_dst)
                    if val['size'] == st.st_size:
                        self.__post_store(md5id, tmp_dst, src, -1, val)
                        return True
                try_cur += 1
        except IOError as e:
            LOG.error("copy local file error: %s, %s" % (str(val), e.message))
        return False

    def __store_from_remote(self, peer_name, peer_ip, md5id, val, try_max=5):
        assert (val['src'] and val['dst'])
        n = 0
        try:
            rlevel = val.get('store_level', 3)
            if self.store_level > rlevel:
                return False
            file_client = SimpleFileClient(peer_ip, 8073)
            try_cur = 0
            src = val['src']
            tmp_dst = val['dst']
            size = val['size']
            while try_cur < try_max:
                n = file_client.pull(src, tmp_dst, size)
                if size == n:
                    self.__post_store(md5id, tmp_dst, src, size, val)
                    return True
                try_cur += 1
                time.sleep(1)
        except ValueError as e:
            LOG.error("store from remote: %s %s error: %s" % (peer_ip, str(val), e.message))
        except IOError as e:
            LOG.error("pull file error: %s" % str(val))
        return False

    def logical_remove(self, src, val):
        pass

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
