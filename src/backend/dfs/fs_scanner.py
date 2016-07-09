import os
import time
import sys
import threading
import ConfigParser
import ctypes
import hashlib
from dfs_log import LOG
def get_tid():
    tid = ctypes.CDLL('libc.so.6').syscall(186)
    return tid


class FSScanner(threading.Thread):
    auto_interval = True    # auto scan interval

    def __init__(self, to_monitor, scan_interval=10, scan_del_interval=10, need_thread=True):
        self.to_monitor = to_monitor
        self.scan_interval = scan_interval
        self.scan_del_interval = scan_del_interval
        self.last_status = None
        self.add_files = None
        self.del_files = None
        self.is_shutdown = False
        self.last_ctime = 0
        self.inited = False
        if need_thread:
            threading.Thread.__init__(self)

    def read_config(self, root):
        config_path = os.path.join(root, '.simple.dfs.config')
        config_parser = None
        if os.path.exists(config_path):
            config_parser = ConfigParser.SafeConfigParser()
            config_path.read(config_path)
        return config_parser

    def scan_once(self, skip_hidden=True):
        return self.start_scan(self.to_monitor, default_store_level=3, skip_hidden=skip_hidden)

    def start_scan(self, dirname, default_store_level, skip_hidden=True):
        max_ctime = 0
        cur_status = {}
        for (root, dirnames, filenames) in os.walk(dirname):
            config_parser = self.read_config(root)
            store_level = default_store_level
            if config_parser is not None:
                store_level = config_parser.get('base', 'store_level')

            for dn in dirnames:
                st = os.stat(os.path.join(root, dn))
                if st.st_ctime > max_ctime:
                    max_ctime = st.st_ctime
                sub_status, sub_ctime = self.start_scan(os.path.join(root, dn), store_level, skip_hidden=skip_hidden)
                if sub_status is not None:
                    if sub_ctime > max_ctime:
                        max_ctime = sub_ctime
                    cur_status.update(sub_status)
            for file_name in filenames:
                if skip_hidden and file_name[0] == ".":
                    continue
                h = hashlib.md5()
                name = os.path.join(root, file_name)
                st = os.stat(name)
                h.update("%s:%d" % (file_name, st.st_ino))
                md5id = h.hexdigest()
                if st.st_ctime > max_ctime:
                    max_ctime = st.st_ctime
                val = {'mtime': st.st_ctime,
                       'md5id': md5id,
                       'size': st.st_size,
                       'ino': st.st_ino,
                       'src': name,
                       'dst': '',
                       'store_level': store_level
                       }
                cur_status[md5id] = val
        return cur_status, max_ctime

    def get_files_meta(self):
        return self.last_ctime, self.last_status

    def diff_status(self, cur_status):
        add_files = {}
        del_files = {}
        if self.last_status is None:
            self.last_status = cur_status
            add_files = cur_status
        else:
            cur_copy = cur_status.copy()
            for (k, v) in self.last_status.items():
                if k in cur_status:
                    # previou exists
                    if v != cur_status[k]:
                        del_files[k] = v
                        add_files[k] = cur_status[k]
                    del cur_copy[k]
                else:
                    del_files[k] = v
            add_files.update(cur_copy)
        # TODO pickle dump
        self.last_status = cur_status
        return del_files, add_files

    def shutdown(self):
        self.is_shutdown = True

    def first_inited(self):
        return self.inited

    def run(self):
        LOG.debug("FS Scanner started: %s" % self.to_monitor)
        last_ts = int(time.time())
        last_del_ts = int(time.time())
        self.inited = False
        while not self.is_shutdown:
            cur_ts = int(time.time())
            big_scan = (cur_ts - last_del_ts) > self.scan_del_interval

            cur_status, last_ctime = self.start_scan(self.to_monitor, default_store_level=3)
            self.inited = True
            if last_ctime > self.last_ctime or big_scan:
                self.last_ctime = last_ctime
                self.del_files, self.add_files = self.diff_status(cur_status)
                if len(self.del_files) > 0:
                    self.last_ctime += 0.01
                if big_scan:
                    last_del_ts = cur_ts
            else:
                pass

            cur_ts = int(time.time())
            while (cur_ts - last_ts) < self.scan_interval:
                cur_ts = int(time.time())
            last_ts = cur_ts
            time.sleep(self.scan_interval)


if __name__ == '__main__':
    scanner = FSScanner("/home/erwin/cbuild", scan_interval=15)
    scanner.start()
    scanner.join()