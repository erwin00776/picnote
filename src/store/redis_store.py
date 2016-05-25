import json
import redis
import datetime

class RedisStore:
    def __init__(self):
        self.r = redis.Redis(port=6379)

    def __init__(self, addr="127.0.0.1", port=6379, db=0):
        self.r = redis.Redis(host=addr, port=port, db=db)

    def __convert_second_tblname(self, tbl):
        return "__st_%s" % tbl

    def __ts2score(self, ts):
        ''' timestamp trim to timestamp of day '''
        dt = datetime.datetime.fromtimestamp(int(ts))
        y, m, d, hh, mm, ss = dt.timetuple()[0:6]
        dt = datetime.datetime(y, m, d, 0, 0, 0)
        score = dt.strftime("%s")
        return score

    def _put_second_index(self, tbl, score, key):
        tbl = self.__convert_second_tblname(tbl)
        self.r.rpush(score, key)
        self.r.zadd(tbl, score, score)

    def _del_second_index(self, tbl, score, key):
        tbl = self.__convert_second_tblname(tbl)
        self.r.zrem(tbl, score)
        self.r.lrem(score, key, num=0)

    def _range_second_index(self, tbl, score_begin, score_end):
        tbl = self.__convert_second_tblname(tbl)
        return self.r.zrangebyscore(tbl, score_begin, score_end)

    def put_timeline(self, ts, key):
        score = self.__ts2score(ts)
        self._put_second_index('timeline', score, key)

    def del_timeline(self, id):
        # score = self.__ts2score(key)
        score = None
        ttime = self.r.hget(id, 'ttime')
        if ttime is not None:
            score = ttime
        else:
            score = self.r.hget(id, 'ctime')
        self._del_second_index('timeline', score, id)

    def range_timeline(self, date_begin, date_end):
        dt1 = datetime.datetime(int(date_begin[:4]), int(date_begin[4:6]), int(date_begin[6:]))
        dt2 = datetime.datetime(int(date_end[:4]), int(date_end[4:6]), int(date_end[6:]))
        score_begin = dt1.strftime("%s")
        score_end = dt2.strftime("%s")
        print(score_begin, score_end)
        date_list = self._range_second_index('timeline', score_begin, score_end)
        pics_list = []
        for d in date_list:
            tmplist = self.r.lrange(d, '00', '99')
            pics_list = pics_list + tmplist
        return pics_list


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

    def replace_meta(self, id, f, v):
        try:
            if isinstance(v, tuple):
                v = json.dumps(v)
            self.r.hset(id, f, v)
        except Exception as e:
            pass

    def del_meta(self, id):
        self.r.delete(id)

    def get_meta_field(self, id, field):
        field_val = self.r.hget(id, field)
        return field_val

    def get_meta(self, id):
        return self.r.hgetall(id)

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

    def get_avail_dates(self):
        date_list = self._range_second_index('timeline', 0, 9999999999)
        dates = {}
        for ts in date_list:
            dt = datetime.datetime.fromtimestamp(int(ts))
            y, m, d, hh, mm, ss = dt.timetuple()[0:6]
            month_list = dates.get(y, [])
            month_list.append(m)
            dates[y] = month_list
        for (y, month_list) in dates.items():
            dates[y] = sorted(set(month_list))
        return dates



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
