import os
import time
import sys
import threading
import ConfigParser
import ctypes


def get_tid():
    tid = ctypes.CDLL('libc.so.6').syscall(186)
    return tid


class FSScanner(threading.Thread):
    auto_interval = True    # auto scan interval

    def __init__(self, to_monitor, scan_interval=10, scan_del_interval=10):
        self.to_monitor = to_monitor
        self.scan_interval = scan_interval
        self.scan_del_interval = scan_del_interval
        self.last_status = None
        self.add_files = None
        self.del_files = None
        self.is_shutdown = False
        self.last_ctime = 0
        threading.Thread.__init__(self, name="FileSystemScanner-%d" % get_tid())

    def read_config(self, root):
        config_path = os.path.join(root, '.simple.dfs.config')
        config_parser = None
        if os.path.exists(config_path):
            config_parser = ConfigParser.SafeConfigParser()
            config_path.read(config_path)
        return config_parser

    def scan_once(self):
        return self.start_scan(self.to_monitor, default_store_level=3)

    def start_scan(self, dirname, default_store_level):
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
                sub_status, sub_ctime = self.start_scan(os.path.join(root, dn), store_level)
                if sub_status is not None:
                    if sub_ctime > max_ctime:
                        max_ctime = sub_ctime
                    cur_status.update(sub_status)
            for fn in filenames:
                name = os.path.join(root, fn)
                st = os.stat(name)
                if st.st_ctime > max_ctime:
                    max_ctime = st.st_ctime
                vals = {'mtime': st.st_ctime,
                        'size': st.st_size,
                        'inode': st.st_ino,
                        'src_path': name,
                        'dst_path': '',
                        'store_level': store_level
                        }
                cur_status[name] = vals
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
        print('del ', del_files)
        print('add ', add_files)

        return del_files, add_files

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        print("FS Scanner started: %s" % self.to_monitor)
        last_ts = int(time.time())
        last_del_ts = int(time.time())
        while not self.is_shutdown:
            cur_ts = int(time.time())
            big_scan = (cur_ts - last_del_ts) > self.scan_del_interval

            cur_status, last_ctime = self.start_scan(self.to_monitor, default_store_level=3)
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