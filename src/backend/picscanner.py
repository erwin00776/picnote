import os
import hashlib
import math
import json
import logging
import inotify
import inotify.adapters
import sys
import datetime
import time
from PIL import Image
sys.path.append("..")
from store.redis_store import RedisStore
from thumbnail_helper import ThumbnailHelper


_DEFAULT_LOG_FORMAT = '%(levelname)s-%(asctime)s-%(name)s-%(message)s'
LOGGER = logging.getLogger(__name__)

class PicScanner:
    def __init__(self, dirpath, thumbnail_dir="/home/erwin/data/thumbnails"):
        self.dirpath = dirpath
        self.store = RedisStore()
        self.thumbnail_helper = ThumbnailHelper(basedir=thumbnail_dir)
        self._configure_logging()
        self.tmp_modified_files = []
        self.modified_files = []
        self.MODIFIED_TIME_MAX = 15  # seconds
        self.MODIFIED_TIME_MIN = 0.5  # seconds

    def _configure_logging(self):
        LOGGER.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
        sh.setFormatter(formatter)
        LOGGER.addHandler(sh)

    def watch_dir(self, dirnames):
        '''
        :param dirnames:
        :return:
         type 1: add
         type 2: del
         type 3: rename
        '''
        ino = inotify.adapters.Inotify()
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

    def scan_all_dir(self, dirnames):
        ''' scan a directory list '''
        for dirname in dirnames:
            self.scan_dir(dirname)

    def scan_dir(self, dirname):
        ''' scan a directory '''
        files = os.listdir(dirname)
        for filename in files:
            if os.path.isdir(filename):
               continue
            try:
                path = os.path.join(dirname, filename)
                path_thumbnail = os.path.join(dirname, 'thumbnails', "thumbnail_" + filename)
                st = os.stat(path)

                im = Image.open(path)
                try:
                    exif = im._getexif()
                except AttributeError:
                    exif = None

                md5 = hashlib.md5()
                md5.update(filename)
                '''
                x, y = im.size
                scale = int(math.ceil(x * 1.0 / 512.0))
                x1, y1 = int(x / scale), int(y / scale)
                im2 = im.resize((x1, y1))
                im2.save(path_thumbnail)
                '''

                id = "%d-%s" % (int(st.st_ctime), md5.hexdigest())
                thumbnailpath = self.thumbnail_helper.add_thumbnail(id, filename, im)
                if thumbnailpath is None:
                    print("Error! Can not create thumbnail.")
                im.close()

                ttime, latitude, longitude = st.st_ctime, None, None
                if exif is not None:
                    if 306 in exif:
                        ttime = str(exif[306])  # photo token time
                        dt = datetime.datetime.strptime(ttime, "%Y:%m:%d %H:%M:%S")
                        ttime = dt.strftime("%s")
                    if 34853 in exif:
                        latitude_ref = exif[34853][1]
                        latitude = exif[34853][2]
                        longitude_ref = exif[34853][3]
                        longitude = exif[34853][4]
                    #print('exif', id, latitude, st.st_ctime, exif)

                meta = {"ctime": st.st_ctime,
                        "atime": st.st_atime,
                        "mtime": st.st_mtime,
                        "ttime": ttime,
                        "filename": filename,
                        "filepath": path,
                        "thumbnail": thumbnailpath,
                        "size": im.size,
                        "desc": "",
                        "latitude": latitude,
                        "longitude": longitude,
                        }
                self.store.append_id(id, path)
                self.store.set_meta(id, meta)
                self.store.put_timeline(ttime, id)
                if latitude is not None:
                    LOGGER.info("#-- %s", id)

                #print(filename, int(st.st_ctime), im.size, md5.hexdigest())
            except IOError as e:
                print("error: " + str(e))

    def run(self):
        self.scan_dir(self.dirpath)


if __name__ == '__main__':
    # start a process to scan
    dirpath = "/home/erwin/pictures"
    pic_scanner = PicScanner(dirpath)
    pic_scanner.run()
    #pic_scanner.watch_dir([dirpath])