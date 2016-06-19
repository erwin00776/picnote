import threading
import SocketServer
import socket
import select
import errno

def _eintr_retry(func, *args):
    """restart a system call interrupted by EINTR"""
    while True:
        try:
            return func(*args)
        except (OSError, select.error) as e:
            if e.args[0] != errno.EINTR:
                raise

class SimpleBaseHandler():
    def __init__(self, request, client_addr, server):
        self.requset = request
        self.client_addr = client_addr
        self.server = server

    def setup(self):
        pass

    def handle(self):
        pass

    def finish(self):
        pass


class SimpleBaseServer():
    def __init__(self):
        pass


class FileHandler(SimpleBaseHandler):
    def __init__(self, request, client_address, server):
        self.server = server
        SimpleBaseHandler.__init__(self, request, client_address, server)

    def handle(self):
        self.data = self.request.recv(1024).strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        self.request.sendall(self.data.upper())

    def finish(self):
        print("finish", self.data)

class FileServer(threading.Thread):
    address_family = socket.AF_INET
    socket_type = socket.SOCK_STREAM
    request_queue_size = 5
    allow_reuse_address = False
    __is_shutdown = threading.Event()
    __shutdown_request = False

    def __init__(self, server_address, auto_start=True, HandlerClass=SimpleBaseHandler):
        self.HandlerClass = HandlerClass
        self.server_address = server_address
        self.socket = socket.socket(self.address_family,
                                    self.socket_type)
        if auto_start:
            try:
                self.server_bind()
                self.server_activate()
            except:
                self.server_close()
                raise

    def server_bind(self):
        if self.allow_reuse_address:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        self.server_address = self.socket.getsockname()

    def server_activate(self):
        self.socket.listen(self.request_queue_size)

    def shutdown(self):
        self.__shutdown_request = True
        self.__is_shutdown.wait()

    def server_close(self):
        if self.socket is not None:
            self.socket.close()

    def handle_timeout(self):
        pass

    def handle_request(self, request, client_addr):
        try:
            print("start a request handler 0 ")
            timeout = self.socket.gettimeout()
            if timeout is None:
                timeout = self.timeout
            elif self.timeout is not None:
                timeout = min(timeout, self.timeout)
            fd_sets = _eintr_retry(select.select, [self], [], [], timeout)
            if not fd_sets[0]:
                self.handle_timeout()
                return

            print("start a request handler")
            handler = self.HandlerClass(request, client_addr, self)
            handler.setup()
            handler.handle()
            self.finish_request()
        except:
            self.shutdown_request(handler)

    def finish_request(self, handler):
        handler.finish()

    def shutdown_request(self, handler):
        try:
            handler.requset.shutdown()
        except:
            pass
        self.close_request(handler)

    def close_request(self, handler):
        handler.requset.close()

    def __handle_request(self):
        request, client_addr = self.socket.accept()
        self.handle_request(request, client_addr)

    def fileno(self):
        return self.socket.fileno()

    def serve_forever(self, poll_interval=0.5):
        self.__is_shutdown.clear()
        print("serve forever")
        try:
            while not self.__shutdown_request:
                r, w, e = _eintr_retry(select.select, [self], [], [],
                                       poll_interval)
                if r is self:
                    print("hi")
                    self.__handle_request()
        finally:
            self.__shutdown_request = False
            self.__is_shutdown.set()
        print("game over.")


class FileClient:
    def __init__(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((ip, port))

    def send(self, data):
        try:
            self.sock.send(data)
            # response = self.sock.recv(64)
        finally:
            pass

    def close(self):
        if self.sock is not None:
            self.sock.close()


class SimpleSrv():
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("localhost", 8088))
        self.sock.listen(5)

    def handle(self, request, client_addr):
        done = False
        h = request.recv(1024)
        h = h.strip()
        print("recv %s" % h)
        import json
        header = json.loads(h)
        filename = header['filename']
        size = header['size']
        n = 0
        while n < size:
            try:
                data = request.recv(1024)
                if not data:
                    done = True
                    break
                n += len(data)
                print(n)
                #request.send(data.upper())
            except:
                done = True
                request.shutdown()
                request.close()
        if n != size:
            print("recv error.")
        print("recv %d, expect: %d", n, size)

    def run(self):
        while True:
            try:
                request, client_addr = self.sock.accept()
                self.handle(request, client_addr)
            except IOError as e:
                print("accept error", e.message)

if __name__ == "__main__":
    srv = SimpleSrv()
    srv.run()