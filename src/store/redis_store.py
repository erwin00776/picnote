import json
import redis
import datetime

class RedisStore:
    def __init__(self):
        self.r = redis.Redis(port=6379)

    def append_id(self, id, path):
        ''' [id0, id1, id...] '''
        self.r.rpush("pic_ids", id)
        # self.put_id2path(id, path)

    def append_modified_files(self, fileattrs):
        attrs = json.dumps(fileattrs)
        self.r.rpush("modified", attrs)

    def set_meta(self, id, map_val):
        for (field, v) in map_val.items():
            if isinstance(v, tuple):
                v = json.dumps(v)
            try:
                self.r.hset(id, field, str(v))
            except Exception as ex:
                print('err', id, field, v, str(ex))

    def replace_meta(self, id, field, val):
        pass

    def get_meta(self, id):
        return self.r.get(id)

    def put_id2path(self, id, path):
        ''' id->pic_path '''
        self.r.set(id, path)

    def get_id2path(self, id):
        return self.r.get(id)

    def get_by_timeline(self, year_range, month_range):
        y, m, d, hh, mm, ss, _ = datetime.datetime.now()
        if year_range is None and month_range is not None:
            start_dt = datetime(y, month_range[0], 1, 0, 0, 0)
            end_dt = datetime(y, month_range[1], d, 23, 59, 59)
        elif year_range is not None and month_range is None:
            start_dt = datetime(year_range[0], 1, 1, 0, 0, 0)
            end_dt = datetime(year_range[1], 12, 31, 23, 59, 59)
        else:
            start_dt = datetime(y, 1, 1, 0, 0, 0)
            end_dt = datetime(y, m, d, 23, 59, 59)
        start_ts = start_dt.strftime("%s")
        end_ts = end_dt.strftime("%s")



if __name__ == '__main__':
    import os
    import hashlib
    import math
    from PIL import Image

    dirname = '/home/erwin/pictures'
    files = os.listdir(dirname)
    for filename in files:
        if not os.path.isdir(filename):
            path = os.path.join(dirname, filename)
            path_thumbnail = os.path.join(dirname, 'thumbnails', "thumbnail_"+filename)
            st = os.stat(path)
            im = Image.open(path)
            md5 = hashlib.md5()
            md5.update(filename)
            x, y = im.size
            scale = int(math.ceil(x*1.0/512.0))
            x1, y1 = int(x/scale), int(y/scale)
            im2 = im.resize((x1, y1))
            im2.save(path_thumbnail)
            print(filename, int(st.st_ctime), im.size, md5.hexdigest())
