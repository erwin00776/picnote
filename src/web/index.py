import web
import os
import sys
from web.httpserver import StaticMiddleware

urls = (
    '/', 'index',
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
