__author__ = 'erwin'

import datetime
import hashlib
import logging
import os
import sys
import threading
import time
from PIL import Image

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore
from src.backend.handlers.words_processors import PicturesNote
from thumbnail_helper import ThumbnailHelper


class PictureHandler(FileSystemEventHandler):
    PIC_NOTE_BATCH = 100
    PIC_NOTE_WAIT_TIME = 60

    def __init__(self, redis_db=None, auto_connect=True):
        self.store = None
        self.thumbnail_helper = ThumbnailHelper(basedir=THUMBNAILS_PATH)
        if auto_connect:
            db = REDIS_DB
            if redis_db is None:
                db = redis_db
            self.store = RedisStore(REDIS_ADDR, REDIS_PORT, db)
        self.waiting_list = {}
        self.waiting_last_time = int(time.time())
        self.picture_note = PicturesNote(redis_db=redis_db)

    def on_created(self, event):
        what = "directory" if event.is_directory else "file"
        if not event.is_directory:
            print("create", event.src_path)
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

    def _fid(self, p):
        filename = os.path.basename(p)
        md5 = hashlib.md5()
        md5.update(filename)
        id = md5.hexdigest()
        return id

    def created(self, file_path):
        """
        Store
            fid->meta{}
            timeline: second index
        :param file_path: created file path
        :return: None
        """
        try:
            filename = os.path.basename(file_path)
            md5 = hashlib.md5()
            md5.update(filename)

            im = Image.open(file_path)
            try:
                exif = im._getexif()
            except AttributeError:
                exif = None

            st = os.stat(file_path)
            # id = "%d-%s" % (int(st.st_ctime), md5.hexdigest())
            fid = self._fid(file_path)
            thumbnail_path = self.thumbnail_helper.add_thumbnail(fid, filename, im)
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
                    "filepath": file_path,
                    "thumbnail": thumbnail_path,
                    "size": im.size,
                    "desc": "",
                    "latitude": latitude,
                    "longitude": longitude,
                    }
            # self.store.append_id(fid, path)
            self.store.set_meta(fid, meta)
            self.store.put_timeline(ttime, fid)
            self.waiting_list[fid] = file_path
            if latitude is not None:
                LOGGER.info("#-- %s", fid)
        except IOError as e:
            print("error: " + str(e))
        time_elasped = int(time.time()) - self.waiting_last_time
        # batch update or update for a period.
        if time_elasped >= self.PIC_NOTE_WAIT_TIME or len(self.waiting_list) >= self.PIC_NOTE_BATCH:
            self.generate_descs(self.waiting_list)
            self.waiting_list = {}
            self.waiting_last_time = int(time.time())

    def renamed(self, src, dest):
        """
        :param path:
        :return:
        """
        id1 = self._fid(src)
        id2 = self._fid(dest)

        vals = self.store.get_meta(id1)
        if vals is None or len(vals) < 1:
            # TODO
            pass
        self.store.set_meta(id2, vals)
        self.store.del_meta(id1)
        self.store.del_timeline(id1)
        ts = vals.get('ttime', None)
        if ts is None:
            ts = vals.get('ctime')
        try:
            self.store.put_timeline(ts, id2)
        except:
            LOGGER.error("can not put timeline: " + str(vals))
        LOGGER.debug("renamed id: %s %s", src, dest)

    def deleted(self, src):
        """
        :param src:
        :return:
        """
        id = self._fid(src)
        self.thumbnail_helper.del_thumbnail(id, src)
        self.store.del_timeline(id)
        self.store.del_meta(id)
        LOGGER.debug("deleted id: " + id)

    def generate_descs(self, fid2file):
        """
        :return:
        """
        pic_file = '/tmp/pic_note_list.txt'
        pic_list_file = open(pic_file, 'w')
        for (fid, file_path) in fid2file.items():
            pic_list_file.write("%s\n" % file_path)
        pic_list_file.close()
        self.picture_note.gen_notes_by_list(pic_file, len(fid2file))


class PictureWatcher(threading.Thread):
    paths = []
    event_handler = None
    observer = None

    def init(self, paths, handler):
        self.paths = paths
        print(self.paths)
        self.event_handler = handler
        self.observer = Observer()

    def run(self):
        for dir_path in self.paths:
            print("watching", dir_path)
            self.observer.schedule(self.event_handler, dir_path, recursive=True)
        self.observer.start()
        print("picture watcher started.")
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
    path = sys.argv[1] if len(sys.argv) > 1 else '/home/erwin/Pictures/nerual_talk'
    # event_handler = LoggingEventHandler()
    picture_handler = PictureHandler(redis_db=6)
    watcher = PictureWatcher()
    watcher.init([path], handler=picture_handler)
    try:
        watcher.start()
    except threading.ThreadError as e:
        print("Pictures Watcher Error: %s" % e.message)
