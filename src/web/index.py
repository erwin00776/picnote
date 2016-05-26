import web
import os
import sys
sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore
from backend.words_query import WordsQuery
from web.httpserver import StaticMiddleware

DAYS_OF_MONTH = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

urls = (
    '/', 'index',
    '/get_by_timeline/(.*)-(.*)', 'GetByTimeline',
    '/get_by_kw/(.*)', 'GetByKw',
    '/(.*.jpg)', 'static_jpg',
    '/(.*.JPG)', 'static_jpg',
)

class static_jpg:
    def GET(self, file):
        try:
            f = open(file, 'r')
            print('## '+file)
            return f.read()
        except:
            return ''


class GetByKw:
    def GET(self, params):
        r = RedisStore()
        wq = WordsQuery()
        render = web.template.render('templates/')
        words = params.split('&')
        pics = wq.query(words)
        thumbnails = []
        captions = []
        for pic_id in pics:
            thumbnail = r.get_meta_field(pic_id, 'thumbnail')
            # convert real path to soft-link of static.
            if thumbnail is None:
                # add 404.
                continue
            suffix = thumbnail[thumbnail.find('thumbnails'):]
            softpath = os.path.join('../../static', suffix)
            thumbnails.append(softpath)
            captions.append(r.get_meta_field(pic_id, 'desc'))
        return render.get_by_kw({}, words, captions, thumbnails)


class GetByTimeline:
    def GET(self, param1, param2):
        r = RedisStore()
        render = web.template.render('templates/')
        pics_list = r.range_timeline(date_begin=param1, date_end=param2)
        thumbnails = []
        for pic_id in pics_list:
            thumbnail = r.get_meta_field(pic_id, 'thumbnail')
            # convert real path to soft-link of static.
            if thumbnail is None:
                # add 404.
                continue
            suffix = thumbnail[thumbnail.find('thumbnails'):]
            softpath = os.path.join('../../static', suffix)
            thumbnails.append(softpath)
        dates = r.get_avail_dates()
        dates_link = {}
        for (y, mlist) in dates.items():
            mlist_link = []
            for m in mlist:
                qrange = "%02d%02d01-%02d%02d%02d" % (y, m, y, m, DAYS_OF_MONTH[m-1])
                mlist_link.append(["%02d" % m, qrange])
            dates_link[y] = mlist_link
        return render.get_by_timeline(param1, param2, dates_link, thumbnails)


class index:
    def load_dir(self):
        basedir = SYSPATH_PREFIX + '/pictures'
        basedir = SYSPATH_PREFIX + '/PycharmProjects/picnote/src/web/static'
        print(basedir)
        pics = []
        for f in os.listdir(basedir):
            basename = os.path.basename(f)
            #pics.append(os.path.join(basedir, f))
            pics.append('static/'+f)
            #print(f)
        return pics

    def GET(self):
        render = web.template.render('templates/')
        name = 'erwin'
        pics = self.load_dir()
        return render.index(name, pics)


if __name__ == "__main__":
    app = web.application(urls, globals())
    application = app.wsgifunc(StaticMiddleware)

    app.run()
