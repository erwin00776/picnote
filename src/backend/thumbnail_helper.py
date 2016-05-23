import os
import math


class ThumbnailHelper:
    def __init__(self, basedir, base_ratio=512.0):
        self.basedir = basedir
        self.base_ratio = base_ratio
        self.PREFIX_LEN = 2

    def add_thumbnail(self, id, filepath, im):
        try:
            filename = os.path.basename(filepath)
            ts, md5 = id.split('-')
            prefix = md5[:self.PREFIX_LEN]
            dirpath = os.path.join(self.basedir, prefix)
            if not os.path.exists(dirpath):
                os.mkdir(dirpath)

            x, y = im.size
            scale = int(math.ceil(x * 1.0 / self.base_ratio))
            x1, y1 = int(x / scale), int(y / scale)
            thumbpath = os.path.join(dirpath, filename)
            im2 = im.resize((x1, y1))
            im2.save(thumbpath)
            im2.close()
        except IOError as e:
            return None
        return thumbpath

    def del_thumbnail(self, filepath):
        filename = os.path.basename(filepath)
        ts, md5 = filename.split('-')
        prefix = md5[:self.PREFIX_LEN]
        dirpath = os.path.join(self.basedir, prefix)
        if not os.path.exists(dirpath):
            return
        os.remove(os.path.join(dirpath, filename))