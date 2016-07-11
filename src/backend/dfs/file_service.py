import copy_reg
import json
import os
import socket
import stat
import struct
import threading
import types

from src.backend.utils.dfs_log import LOG


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


class TCPClient:
    SIZE = 4096

    def __init__(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((ip, port))

    def recv(self, size):
        bs = None
        try:
            bs = self.sock.recv(size)
        except IOError as e:
            LOG.debug(e.message)
            bs = None
        finally:
            return bs

    def send(self, data):
        ok = True
        try:
            self.sock.send(data)
        except IOError as e:
            LOG.debug(e.message)
            ok = False
        finally:
            return ok

    def close(self):
        if self.sock is not None:
            self.sock.close()


class SimpleFileClient(TCPClient):
    def __init__(self, ip, port):
        TCPClient.__init__(self, ip, port)

    def push(self, local_path):
        self.send('push')

        st = os.stat(local_path)
        fin = open(local_path, 'r')
        file_vals = {'src': local_path, 'size': st.st_size, 'id': "xx"}
        header = json.dumps(file_vals)

        header_len = struct.pack(">I", len(header))
        self.send(header_len)
        self.send(header)

        buf = fin.read(self.SIZE)
        n = len(buf)
        while buf:
            ok = self.send(buf)
            if not ok:
                pass
            buf = fin.read(self.SIZE)
            n += len(buf)
        self.close()

    def pull(self, remote_path, local_path):
        self.send('pull')

        file_vals = {'src': remote_path}
        header = json.dumps(file_vals)
        header_len = struct.pack(">I", len(header))
        self.send(header_len)
        self.send(header)

        fout = open(local_path, 'w')
        n = 0
        buf = self.recv(self.SIZE)
        while buf:
            if buf is None or len(buf) <= 0:
                break
            fout.write(buf)
            n += len(buf)
            buf = self.recv(self.SIZE)
        LOG.debug("recv: %s %d" % (remote_path, n))
        fout.flush()
        fout.close()
        os.chmod(local_path, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP + stat.S_IWGRP + stat.S_IROTH)


class SimpleBaseHandler(threading.Thread):
    SIZE = 4096

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


class ReadFileHandler(SimpleBaseHandler):
    """
    Client <- Server
       --> header_len | header{}
       <-- content
    """
    def handle(self):
        LOG.debug("[%s] start handle from %s" % (threading.currentThread().getName(),
                                             str(self.client_addr)))
        done = False
        bs = self.request.recv(4)
        bs = bs.strip()
        header_len = struct.unpack(">I", bs)[0]

        h = self.request.recv(header_len)
        h = h.strip()

        header = json.loads(h)
        filename = header['src']
        if not os.path.exists(filename):
            self.request.close()
            self.job_done = True
            return

        st = os.stat(filename)
        size = header.get('size', st.st_size)
        if size <= 0:
            size = st.st_size

        fin = open(filename, 'r')
        n = 0
        while n < size or not done:
            try:
                data = fin.read(self.SIZE)
                if not data:
                    done = True
                    break
                self.request.send(data)
                n += len(data)
            except:
                done = True
                self.request.shutdown()
                self.request.close()
                fin.close()
        if n != size:
            LOG.debug("recv error.")
        fin.close()
        self.request.close()
        self.job_done = True


class WriteFileHandler(SimpleBaseHandler):
    """
    Client -> Server
    """
    def handle(self):
        LOG.debug("[%s] start handle from %s" % (threading.currentThread().getName(),
                                             str(self.client_addr)))
        done = False
        bs = self.request.recv(4)
        bs = bs.strip()
        header_len = struct.unpack(">I", bs)[0]
        LOG.debug("recv %d" % header_len)

        h = self.request.recv(header_len)
        h = h.strip()
        LOG.debug("recv %s" % h)
        header = json.loads(h)
        filename = header['filename']
        size = header['size']
        dstname = os.path.basename(filename)
        store_path = "/home/erwin/tmp"
        fout = open(os.path.join(store_path, dstname), 'w')
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
            LOG.debug("recv error.")
        LOG.debug("recv %d, expect: %d" % (n, size))
        fout.close()

        n_bs = struct.pack(">I", n)
        # self.request.send(n_bs)
        # self.request.shutdown()
        self.request.close()
        self.job_done = True


class SimpleFileSrv(threading.Thread):
    def __init__(self, ip='localhost', port=8073):
        threading.Thread.__init__(self)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr = (ip, port)
        self.sock.bind(addr)
        self.sock.listen(5)
        self.pool = set([])
        LOG.debug("serve at %s" % str(addr))

    def request_handle(self, request, client_addr):
        LOG.debug("recv request from %s" % str(client_addr))

        while len(self.pool) > 5:
            pool = set([])
            for t in self.pool:
                if not t.job_done:
                    pool.add(t)
            self.pool = pool

        cmd = request.recv(4)
        cmd = cmd.strip()
        handler = None
        if cmd == 'pull':
            handler = ReadFileHandler(request, client_addr, self)
        elif cmd == 'push':
            handler = WriteFileHandler(request, client_addr, self)
        else:
            LOG.debug("can not response cmd: %s" % cmd)

        if handler is not None:
            self.pool.add(handler)
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
                LOG.debug("accept error %s" % e.message)


if __name__ == "__main__":
    srv = SimpleFileSrv()
    # srv.run()
    srv.start()