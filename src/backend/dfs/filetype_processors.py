# coding=utf-8

import os
import sys
import redis
import eyed3


class BaseProcessor:
    def __init__(self, store_path, sub, redis_cli):
        self.store_path = os.path.join(store_path, sub)
        self.redis_cli = redis_cli
        self.basename = None
        self.allow_suffix = set([])

    def match(self, filename):
        self.basename = os.path.basename(filename)
        dot = self.basename.rfind('.')
        if dot > -1 and self.basename[dot+1:].lower() in self.allow_suffix:
            return True
        return False

    def process(self, md5id, filename):
        raise "not implement yet."


class DefaultProcessor(BaseProcessor):
    def __init__(self, store_path, sub, redis_cli):
        BaseProcessor.__init__(self, store_path, sub, redis_cli)

    def match(self, filename):
        """ allow process all type of files. """
        BaseProcessor.match(self, filename)
        return True

    def process(self, md5id, filename):
        self.match(filename)
        return True, os.path.join(self.store_path, self.basename)


class PhotoProcessor(BaseProcessor):
    def __init__(self, store_path, sub, redis_cli):
        BaseProcessor.__init__(self, store_path, sub, redis_cli)
        self.allow_suffix = set(['jpg'])

    def process(self, md5id, filename):
        if not self.match(filename):
            return False, None
        dst = os.path.join(self.store_path, md5id[:2], md5id[2:4], self.basename)
        return True, dst


class DocProcessor(BaseProcessor):
    def __init__(self, store_path, sub, redis_cli):
        BaseProcessor.__init__(self, store_path, sub, redis_cli)
        self.allow_suffix = set(['doc'])

    def process(self, md5id, filename):
        if not self.match(filename):
            return False, None
        dst = os.path.join(self.store_path, md5id[:2], self.basename)
        return True, dst


class PdfProcessor(BaseProcessor):
    def __init__(self, store_path, sub, redis_cli):
        BaseProcessor.__init__(self, store_path, sub, redis_cli)
        self.allow_suffix = set(['pdf'])

    def process(self, md5id, filename):
        if not self.match(filename):
            return False, None
        dst = os.path.join(self.store_path, md5id[:2], self.basename)
        return True, dst


class MusicProcessor(BaseProcessor):
    def __init__(self, store_path, sub, redis_cli):
        BaseProcessor.__init__(self, store_path, sub, redis_cli)
        self.allow_suffix = set(['mp3', 'wma'])

    def process(self, md5id, filename):
        if not self.match(filename):
            return False, None
        # store by artists
        tag = eyed3.load(filename)
        artist = None
        album = None
        try:
            artist = tag.tag.artist
            album = tag.tag.album
            artist = artist.encode('utf-8')
            album = album.encode('utf-8')
        except AttributeError as e:
            pass
        if artist and album:
            dst = os.path.join(self.store_path, str(artist), album, self.basename)
        elif artist and not album:
            dst = os.path.join(self.store_path, str(artist), self.basename)
        elif not artist and album:
            dst = os.path.join(self.store_path, str(artist), self.basename)
        else:
            dst = os.path.join(self.store_path, "unknown", self.basename)
        return True, dst


#                                     --- Main Process ---
class FileTypeHelper:
    def __init__(self, store_path, redis_cli):
        self.store_path = store_path
        self.redis_cli = redis_cli
        self._processors = []
        self._register()

    def _register(self):
        self._processors.append(PhotoProcessor(self.store_path, "photos",  self.redis_cli))
        self._processors.append(PdfProcessor(self.store_path, "pdf",  self.redis_cli))
        self._processors.append(DocProcessor(self.store_path, "doc", self.redis_cli))
        self._processors.append(MusicProcessor(self.store_path, "musics", self.redis_cli))
        self._processors.append(DefaultProcessor(self.store_path, "default", self.redis_cli))

    def process(self, md5id, filename):
        """ it must be return absolute path. """
        dst = None
        # md5id = val['md5id']
        for p in self._processors:
            is_break, dst = p.process(md5id, filename)
            if is_break:
                break
        if dst:
            d = os.path.dirname(dst)
            try:
                if not os.path.exists(d):
                    os.makedirs(d, mode=0775)
            except OSError as e:
                pass
        return dst


if __name__ == "__main__":
    r = redis.Redis()
    file_type = FileTypeHelper("/home/erwin/store", r)
    print(file_type.process("AS1A222", "abc"))
    print(file_type.process("B1A432", "pic.jpg"))
    print(file_type.process("D123A2", "/media/erwin/Fun2/KuGou/杨千嬅 - 未完的歌.mp3"))
    print(file_type.process("E1E3A2", "nlp.pdf"))
    print(file_type.process("E1E3A2", "ml.pdf"))
    print(file_type.process("E1E3A2", "rf.pdf"))
    print(file_type.process("F6431A2", "diary.doc"))
    print(file_type.process("G641A2", "xxx.xxx"))