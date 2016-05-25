__author__ = 'erwin'

import os
import sys
import time
import logging
import threading
import datetime
import hashlib
from PIL import Image
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler

sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore
from thumbnail_helper import ThumbnailHelper

class PictureHandler(FileSystemEventHandler):
    def __init__(self, auto_connect=True):
        self.store = None
        self.thumbnail_helper = ThumbnailHelper(basedir=THUMBNAILS_PATH)
        if auto_connect:
            self.store = RedisStore(REDIS_ADDR, REDIS_PORT, REDIS_DB)

    def on_created(self, event):
        what = "directory" if event.is_directory else "file"
        if not event.is_directory:
            self.created(event.src_path)

    def on_modified(self, event):
        what = "directory" if event.is_directory else "file"

    def on_moved(self, event):
        what = "directory" if event.is_directory else "file"
        if not event.is_directory:
            self.renamed(event.src_path, event.dest_path)


    def on_deleted(self, event):
        what = "directory" if event.is_directory else "file"
        if not event.is_directory:
            self.deleted(event.src_path)

    def _gid(self, p):
        filename = os.path.basename(p)
        md5 = hashlib.md5()
        md5.update(filename)
        id = md5.hexdigest()
        return id

    def created(self, path):
        """
        Store
            id""->meta{}
            timeline: second index
        :param path: created file path
        :return: None
        """
        try:
            filename = os.path.basename(path)
            md5 = hashlib.md5()
            md5.update(filename)

            im = Image.open(path)
            try:
                exif = im._getexif()
            except AttributeError:
                exif = None

            st = os.stat(path)
            # id = "%d-%s" % (int(st.st_ctime), md5.hexdigest())
            id = self._gid(path)
            thumbnail_path = self.thumbnail_helper.add_thumbnail(id, filename, im)
            if thumbnail_path is None:
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

            meta = {"ctime": st.st_ctime,
                    "atime": st.st_atime,
                    "mtime": st.st_mtime,
                    "ttime": ttime,
                    "filename": filename,
                    "filepath": path,
                    "thumbnail": thumbnail_path,
                    "size": im.size,
                    "desc": "",
                    "latitude": latitude,
                    "longitude": longitude,
                    }
            # self.store.append_id(id, path)
            self.store.set_meta(id, meta)
            self.store.put_timeline(ttime, id)
            if latitude is not None:
                LOGGER.info("#-- %s", id)
        except IOError as e:
            print("error: " + str(e))

    def renamed(self, src, dest):
        """
        :param path:
        :return:
        """
        id1 = self._gid(src)
        id2 = self._gid(dest)

        vals = self.store.get_meta(id1)
        self.store.set_meta(id2, vals)
        self.store.del_meta(id1)
        self.store.del_timeline(id1)
        ts = vals['ttime']
        if ts is None:
            ts = vals['ctime']
        self.store.put_timeline(ts, id2)
        LOGGER.debug("renamed id: %s %s", src, dest)

    def deleted(self, src):
        """
        :param src:
        :return:
        """
        id = self._gid(src)
        self.store.del_timeline(id)
        self.store.del_meta(id)
        LOGGER.debug("deleted id: " + id)


class PictureWatcher(threading.Thread):
    def init(self, p, handler):
        self.path = p
        # self.event_handler = PicturesHandler()
        self.event_handler = handler
        self.observer = Observer()

    def run(self):
        self.observer.schedule(self.event_handler, self.path, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '/Users/erwin/tmp'
    # event_handler = LoggingEventHandler()
    watcher = PictureWatcher()
    watcher.init(path)
    try:
        watcher.start()
    except threading.ThreadError as e:
        print("Pictures Watcher Error: %s" % e.message)
