#coding=utf-8
import os

'''
import inotify
import inotify.adapters
'''
import sys
import time
sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore
from picture_watcher import PictureHandler
from picture_watcher import PictureWatcher
from src.backend.handlers.words_processors import PicturesNote


class PictureScanner:
    def __init__(self, dir_list, redis_db=5):
        self.dir_list = dir_list
        self.store = RedisStore(db=redis_db)
        self.handler = PictureHandler(redis_db=redis_db)
        self.pn = PicturesNote(redis_db=redis_db)
        self.tmp_modified_files = []
        self.modified_files = []
        self.MODIFIED_TIME_MAX = 15  # seconds
        self.MODIFIED_TIME_MIN = 0.5  # seconds

    # 暂时没用
    def watch_dir(self, dirnames):
        """
        :param dirnames:
        :return:
         type 1: add
         type 2: del
         type 3: rename
        """
        ino = None
        try:
            for dirname in dirnames:
                ino.add_watch(dirname)
            for ev in ino.event_gen():
                curtime = time.time()
                self.tmp_modified_files = [i for i in self.tmp_modified_files if i is not None]
                for i in range(0, len(self.tmp_modified_files)):
                    e = self.tmp_modified_files[i]
                    if curtime-e[1] > self.MODIFIED_TIME_MAX:
                        self.tmp_modified_files[i] = None
                        continue
                    elif e[0] == 2 and (curtime-e[1]) < self.MODIFIED_TIME_MIN:
                        continue
                    if e[0] == 2 and (curtime-e[1]) < self.MODIFIED_TIME_MAX:
                        # delete, type = 2
                        self.modified_files.append(e)
                        self.tmp_modified_files[i] = None
                for e in self.modified_files:
                    self.store.append_modified_files(e)
                self.modified_files = []

                if ev is not None:
                    (header, type_names, watch_path, filename) = ev
                    if type_names[0] == 'IN_CREATE':
                        # add, type = 1
                        cur = [1, curtime, header.cookie, watch_path, filename]
                        self.modified_files.append(cur)
                    elif type_names[0] == 'IN_MOVED_FROM':
                        self.tmp_modified_files.append([2, curtime, header.cookie, watch_path, filename])
                    elif type_names[0] == 'IN_MOVED_TO':
                        # rename, type = 3
                        cur = [3, curtime, header.cookie, watch_path, filename]
                        rename = False
                        for i in range(0, len(self.tmp_modified_files)):
                            e = self.tmp_modified_files[i]
                            if e is None:
                                continue
                            if e[2] == cur[2] and (cur[1]-e[1] < self.MODIFIED_TIME_MAX):
                                self.tmp_modified_files[i] = None
                                rename = True
                                break
                        if rename:
                            LOGGER.info("found a rename: " + filename)
                            self.modified_files.append(cur)
                    LOGGER.info("WD=(%d), mask=(%d) cookie=(%d) len=(%d) mask->names=%s"
                                " watch-path=[%s], filename=[%s]",
                                header.wd, header.mask, header.cookie, header.len, type_names,
                                watch_path, filename)
        except inotify.calls.InotifyError as err:
            print('inotify error: ' + str(err))
        finally:
            for dirname in dirnames:
                ino.remove_watch(dirname)

    def check_last_scan(self, dirpath):
        if os.path.dirname(dirpath):
            p = os.path.join(dirpath, LAST_SCAN_FILENAME)
            if not os.path.exists(p):
                return True, 0
            ts_dir = int(os.stat(p).st_mtime)
            try:
                with open(p, 'r') as f:
                    ts_last = int(f.readline())
                    if ts_dir - ts_last > 60:
                        return True, ts_last
                    else:
                        return False, 0
            except IOError as e:
                return True, 0
            except ValueError as e:
                return True, 0
        return False, 0

    def update_last_scan(self, dirpath):
        if os.path.dirname(dirpath):
            p = os.path.join(dirpath, LAST_SCAN_FILENAME)
            try:
                fout = open(p, 'w')
                ts = int(os.stat(p).st_mtime)
                fout.write(str(ts)+"\n")
                fout.close()
            except IOError as e:
                if fout is not None:
                    fout.close()

    def scan_dir(self, dir_name):
        """ scan a directory """
        files = os.listdir(dir_name)
        updated, ts_last = self.check_last_scan(dir_name)     # TODO return ts, identy file.
        nums = 0
        for filename in files:
            if os.path.isdir(filename) or filename[-3].lower() in PICTURE_SUFFIXS:
                continue
            path = os.path.join(dir_name, filename)
            st = os.stat(path)
            nums += 1
            if int(st.st_mtime) > ts_last:
                LOGGER.info("adding file %s" % path)
                self.handler.created(path)
        """
        if updated:
            self.update_last_scan(dir_name)
            self.pn.gen_notes(dir_name)
        """

    def run(self):
        for dir_name in self.dir_list:
            print("scan dir: %s" % dir_name)
            self.scan_dir(dir_name)
            self.update_last_scan(dir_name)


if __name__ == '__main__':
    # scan pictures
    dirs = ['/media/erwin/Data1/20160620', '/home/erwin/Pictures/nerual_talk']
    pic_scanner = PictureScanner(dirs)
    pic_scanner.run()

    # inotify pictures
    pic_watcher = PictureWatcher()
    pic_watcher.init(dirs, pic_scanner.handler)
    pic_watcher.start()
    pic_watcher.join()
