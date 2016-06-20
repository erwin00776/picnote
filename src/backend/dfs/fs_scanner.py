import os
import time
import sys
import threading

class FSScanner(threading.Thread):
    auto_interval = True    # auto scan interval

    def __init__(self, to_monitor, scan_interval=600):
        self.to_monitor = to_monitor
        self.scan_interval = scan_interval
        self.last_status = None
        self.is_shutdown = False
        self.last_mtime = 0
        threading.Thread.__init__(self, name="FileSystemScanner-0")

    def start_scan(self, dirname):
        max_mtime = 0
        cur_status = {}
        for (root, dirnames, filenames) in os.walk(dirname):
            for dn in dirnames:
                st = os.stat(os.path.join(root, dn))
                if st.st_mtime > max_mtime:
                    max_mtime = st.st_mtime
                sub_status, sub_mtime = self.start_scan(os.path.join(root, dn))
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
                        'inode': st.st_ino
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
        print('del: ', del_status)
        print('add: ', add_status)
        return del_status, add_status

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        last_ts = int(time.time())
        while not self.is_shutdown:
            cur_status, self.last_mtime = self.start_scan(self.to_monitor)
            delfiles, addfiles = self.diff_status(cur_status)

            cur_ts = int(time.time())
            while (cur_ts - last_ts) < self.scan_interval:
                cur_ts = int(time.time())
            last_ts = cur_ts


if __name__ == '__main__':
    scanner = FSScanner("/home/erwin/cbuild", scan_interval=15)
    scanner.start()
    scanner.join()