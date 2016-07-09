from base_point import BasePoint
import sys
sys.path.append("..")
from fs_scanner import FSScanner
from dfs_log import LOG
import json
import time
import hashlib
import os
import threading


class SourcePoints(threading.Thread):
    def __init__(self, roots, redis_cli):
        threading.Thread.__init__(self, name="SourcePoints")
        self.roots = roots
        self.source_points = []
        self.max_last_ts = 0
        self.metas = {}
        self.exist_roots = []
        self.uids = []
        self.uid2ts = {}
        self.redis_cli = redis_cli
        self.is_shutdown = False
        self.source_points = {}
        self.add_files = {}
        self.del_files = {}

    def hashcode(self, s):
        m = hashlib.md5()
        m.update(s)
        return m.hexdigest()

    def start_workers(self, add_roots, del_roots):
        for root in del_roots:
            self.source_points[root].shutdown()
            del self.source_points[root]
        for root in add_roots:
            sp = SourcePoint(root=root)
            self.source_points[root] = sp

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        self.is_shutdown = False
        while not self.is_shutdown:
            add_roots, del_roots = self.refresh_sources()
            self.start_workers(add_roots, del_roots)
            self.merge_metas()
            time.sleep(5)

    def merge_metas(self):
        metas = {}
        #for uid, sp in zip(self.uids, self.source_points):
        for root, sp in self.source_points.items():
            last_ts, meta = sp.get_files_meta()
            prev_ts = self.uid2ts.get(root, 0)
            if prev_ts < last_ts:
                self._dump(root, meta)
            if last_ts > self.max_last_ts:
                self.max_last_ts = last_ts
            if meta is not None:
                metas.update(meta)
            LOG.debug("merging %s %d" % (root, last_ts))
        add_files = {}
        del_files = self.metas.copy()
        for k, v in metas.items():
            if k in del_files:
                del del_files[k]
            if k not in self.metas:
                add_files[k] = v
        self.metas = metas
        self.add_files, self.del_files = add_files, del_files

    def refresh_sources(self):
        exist_roots = []
        add_roots = []
        del_roots = self.exist_roots
        for root in self.roots:
            if os.path.exists(root):
                exist_roots.append(root)
                if root in self.exist_roots:
                    del_roots.remove(root)
                else:
                    add_roots.append(root)
        self.exist_roots = exist_roots
        return add_roots, del_roots

    def _load(self, root):
        hash = hashlib.md5()
        uid = hash.update(root)
        self.uids.append(uid)
        raw_meta = self.redis_cli.hgetall(uid)
        meta = {}
        for file_name, file_vals in raw_meta.items():
            meta[file_name] = json.loads(file_vals)
        return meta

    def load(self):
        metas = []
        for root in self.roots:
            meta = self._load(root)
            metas.append(meta)
        return metas

    def _dump(self, uid, meta):
        for file_name, file_vals in meta.items():
            raw_vals = json.dumps(file_vals)
            self.redis_cli.hset(uid, file_name, raw_vals)

    def dump(self, uids, metas):
        for uid, meta in zip(uids, metas):
            self._dump(uid, meta)

    def get_metas(self):
        return self.max_last_ts, self.metas, self.add_files, self.del_files


class SourcePoint(BasePoint):
    def __init__(self, root):
        BasePoint.__init__(self)
        self.root = root
        self.last_update_ts = 0
        self.file_metas = {}
        self.fsscanner = FSScanner(root, scan_interval=15)
        self.fsscanner.start()

    def shutdown(self):
        self.fsscanner.shutdown()

    def get_root(self):
        return self.root

    def get_files_meta(self):
        self.last_update_ts, self.file_metas = self.fsscanner.get_files_meta()
        return self.last_update_ts, self.file_metas
