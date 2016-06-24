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

    def __init__(self, to_monitor, scan_interval=600):
        self.to_monitor = to_monitor
        self.scan_interval = scan_interval
        self.last_status = None
        self.is_shutdown = False
        self.last_mtime = 0
        threading.Thread.__init__(self, name="FileSystemScanner-%d" % get_tid())

    def read_config(self, root):
        config_path = os.path.join(root, '.simple.dfs.config')
        config_parser = None
        if os.path.exists(config_path):
            config_parser = ConfigParser.SafeConfigParser()
            config_path.read(config_path)
        return config_parser

    def start_scan(self, dirname, default_store_level):
        max_mtime = 0
        cur_status = {}
        for (root, dirnames, filenames) in os.walk(dirname):
            config_parser = self.read_config(root)
            store_level = default_store_level
            if config_parser is not None:
                store_level = config_parser.get('base', 'store_level')

            for dn in dirnames:
                st = os.stat(os.path.join(root, dn))
                if st.st_mtime > max_mtime:
                    max_mtime = st.st_mtime
                sub_status, sub_mtime = self.start_scan(os.path.join(root, dn), store_level)
                if sub_status is not None:
                    if sub_mtime > max_mtime:
                        max_mtime = sub_mtime
                    cur_status.update(sub_status)
            for fn in filenames:
                name = os.path.join(root, fn)
                #print(name)
                st = os.stat(name)
                if st.st_mtime > max_mtime:
                    max_mtime = st.st_mtime
                vals = {'mtime': st.st_mtime,
                        'size': st.st_size,
                        'inode': st.st_ino,
                        'src_path': name,
                        'dst_path': '',
                        'store_level': store_level
                        }
                cur_status[name] = vals
        return cur_status, max_mtime

    def get_files_meta(self):
        return self.last_mtime, self.last_status

    def diff_status(self, cur_status):
        add_status = {}
        del_status = {}
        if self.last_status is None:
            self.last_status = cur_status
            add_status = cur_status
        else:
            cur_copy = cur_status.copy()
            for (k, v) in self.last_status.items():
                if k in cur_status:
                    # previou exists
                    if v != cur_status[k]:
                        del_status[k] = v
                        add_status[k] = cur_status[k]
                    del cur_copy[k]
                else:
                    del_status[k] = v
            add_status.update(cur_copy)
        # TODO pickle dump
        self.last_status = cur_status
        '''
        print('local del: ', del_status)
        print('local add: ', add_status)
        print('local current status %d' % len(self.last_status))
        '''
        return del_status, add_status

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        print("FS Scanner started: %s" % self.to_monitor)
        last_ts = int(time.time())
        while not self.is_shutdown:
            cur_status, last_mtime = self.start_scan(self.to_monitor, default_store_level=3)
            if last_mtime > self.last_mtime:
                self.last_mtime = last_mtime
                delfiles, addfiles = self.diff_status(cur_status)
            else:
                # print('no changes')
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