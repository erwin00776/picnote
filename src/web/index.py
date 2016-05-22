import web
import os
import sys
sys.path.append("..")
from store.redis_store import RedisStore
from web.httpserver import StaticMiddleware

urls = (
    '/', 'index',
    '/get_by_timeline/(.*)-(.*)', 'GetByTimeline',
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


class GetByTimeline:
    def GET(self, param1, param2):
        r = RedisStore()
        render = web.template.render('templates/')
        pics_list = r.range_timeline(date_begin=param1, date_end=param2)
        thumbnails = []
        for pic_id in pics_list:
            thumbnail = r.get_meta_field(pic_id, 'thumbnail')
            # convert real path to soft-link of static.
            suffix = thumbnail[thumbnail.find('thumbnails'):]
            softpath = os.path.join('../../static', suffix)
            thumbnails.append(softpath)
        return render.get_by_timeline(param1, param2, thumbnails)

class index:
    def load_dir(self):
        basedir = '/home/erwin/pictures'
        basedir = '/home/erwin/PycharmProjects/picnote/src/web/static'
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
