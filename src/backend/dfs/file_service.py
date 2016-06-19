import threading
import SocketServer
import socket
import struct
import select
import os
import sys
import errno
import copy_reg
import types
from multiprocessing import Pool


def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.mro():
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)

copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)


class FileClient:
    def __init__(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((ip, port))

    def recv(self, size):
        bs = None
        try:
            print("start get return")
            bs = self.sock.recv(size)
            print("get return %s" % str(bs))
            # bs = bs.strip()
        except IOError as e:
            print(e.message)
            bs = None
        finally:
            return bs

    def send(self, data):
        ok = True
        try:
            self.sock.send(data)
        except IOError as e:
            print(e.message)
            ok = False
        finally:
            return ok

    def close(self):
        if self.sock is not None:
            self.sock.close()


class SimpleBaseHandler(threading.Thread):
    def __init__(self, request, client_addr, server):
        self.request = request
        self.client_addr = client_addr
        self.server = server
        self.job_done = False
        threading.Thread.__init__(self)

    def finish(self):
        pass

    def handle(self):
        pass

    def run(self):
        self.handle()


class DefaultHandler(SimpleBaseHandler):
    def handle(self):
        print("[%s] start handle from %s" % (threading.currentThread().getName(),
                                             str(self.client_addr)))
        done = False
        bs = self.request.recv(4)
        bs = bs.strip()
        header_len = struct.unpack(">I", bs)[0]
        print("recv %d" % header_len)

        h = self.request.recv(header_len)
        h = h.strip()
        print("recv %s" % h)
        import json
        header = json.loads(h)
        filename = header['filename']
        size = header['size']
        dstname = os.path.basename(filename)
        store_path = "/home/erwin/tmp"
        fout = open(os.path.join(store_path, dstname), 'wb')
        n = 0
        while n < size or not done:
            try:
                data = self.request.recv(1024)
                fout.write(data)
                if not data:
                    done = True
                    break
                n += len(data)
            except:
                done = True
                self.request.shutdown()
                self.request.close()
                fout.close()
        if n != size:
            print("recv error.")
        print("recv %d, expect: %d" % (n, size))
        fout.close()

        n_bs = struct.pack(">I", n)
        # self.request.send(n_bs)
        # self.request.shutdown()
        self.request.close()
        self.job_done = True


class SimpleSrv():
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr = ('localhost', 8088)
        self.sock.bind(addr)
        self.sock.listen(5)
        #self.pool = Pool(10)
        self.pool1 = set([])
        print("serve at %s" % str(addr))

    def request_handle(self, request, client_addr):
        print("recv request from %s" % str(client_addr))

        while len(self.pool1) > 5:
            pool1 = set([])
            for t in self.pool1:
                if not t.job_done:
                    pool1.add(t)
            self.pool1 = pool1
        handler = DefaultHandler(request, client_addr, self)
        self.pool1.add(handler)
        handler.start()

    def request_close(self):
        pass

    def request_timeout(self):
        pass

    def run(self):
        while True:
            try:
                request, client_addr = self.sock.accept()
                self.request_handle(request, client_addr)
            except IOError as e:
                print("accept error", e.message)


if __name__ == "__main__":
    srv = SimpleSrv()
    srv.run()