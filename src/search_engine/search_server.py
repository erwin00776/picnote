import threading
import time
import socket
import select
from Queue import Queue
from query_parser import QueryParser
from index_store import RedisIndexStore
from src.backend.utils.superior_thread import SuperiorThread


class SearchWorker(SuperiorThread):
    def __init__(self, id, task_queue, server):
        self.task_queue = task_queue
        self.is_shutdown = False
        self.server = server
        self.index_store = RedisIndexStore()
        self.query_parser = QueryParser(self.index_store)
        SuperiorThread.__init__(self, daemon=True, name="SearchWorker-%d" % id)

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        while not self.is_shutdown:
            try:
                fileno, request = self.task_queue.get(block=True)
                results = self.query_parser.parse(request)
                response = ""
                for field, val in results.items():
                    response += "%d\t%s\n" % (field, str(val))
                self.server.response_task(fileno, response)
            except IOError as e:
                print("search worker rror: %s" % e.message)


EOL1 = b'\n\n'
EOL2 = b'\n\r\n'


class SearchServer(SuperiorThread):
    def __init__(self, ip, port):
        self.task_queue = Queue(maxsize=20)
        self.ip = ip
        self.port = port
        self.sock = None
        self.epoll = None
        self.is_shutdown = False
        self.connetions, self.requests, self.responses = {}, {}, {}
        self.workers = []
        self.resp_lock = threading.Lock()
        SuperiorThread.__init__(self, name="SearchServer")

    def shutdown(self):
        for w in self.workers:
            w.shutdown()
        self.is_shutdown = True

    def start_service(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(5)
        self.epoll = select.epoll()
        self.epoll.register(self.sock.fileno(), select.EPOLLIN)

    def start_workers(self):
        for i in range(3):
            w = SearchWorker(i, self.task_queue, self)
            self.workers.append(w)
            w.start()

    def dispatch_task(self, fileno, request):
        self.task_queue.put((fileno, request), block=True)

    def response_task(self, fileno, response):
        self.resp_lock.acquire()
        self.responses[fileno] = response
        self.resp_lock.release()

    def run(self):
        self.start_service()
        self.start_workers()
        try:
            while not self.is_shutdown:
                events = self.epoll.poll(1)
                self.resp_lock.acquire()
                for fileno, event in events:
                    if fileno == self.sock.fileno():
                        connetion, addr = self.sock.accept()
                        connetion.setblocking(0)
                        self.epoll.register(connetion.fileno(), select.EPOLLIN)
                        self.connetions[connetion.fileno()] = connetion
                        self.requests[connetion.fileno()] = b''
                        self.responses[connetion.fileno()] = None # b'hello worlds'
                        print("accepted client socket: %d" % (connetion.fileno()))
                    elif event & select.EPOLLIN:
                        self.requests[fileno] += self.connetions[fileno].recv(1024)
                        if EOL1 in self.requests[fileno] or EOL2 in self.requests[fileno]:
                            self.epoll.modify(fileno, select.EPOLLOUT)
                            self.connetions[fileno].setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
                            print('-'*40 + "\n" + self.requests[fileno].decode()[:-2])
                            self.dispatch_task(fileno, self.requests[fileno].decode()[:-2])
                    elif event & select.EPOLLOUT:
                        if not self.responses[fileno] or len(self.responses[fileno]) == 0:
                            continue
                        nbytes = self.connetions[fileno].send(self.responses[fileno])
                        self.responses[fileno] = self.responses[fileno][nbytes:]
                        if len(self.responses[fileno]) == 0:
                            self.connetions[fileno].setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
                            self.epoll.modify(fileno, 0)
                            self.connetions[fileno].shutdown(socket.SHUT_RDWR)
                    elif event & select.EPOLLHUP:
                        self.epoll.unregister(fileno)
                        self.connetions[fileno].close()
                        del self.connetions[fileno]
                        del self.requests[fileno]
                        del self.responses[fileno]
                        print("client socket %d closed" % fileno)
                self.resp_lock.release()
            print("SearchServer prepare to shutdown.")
        finally:
            self.epoll.unregister(self.sock.fileno())


if __name__ == "__main__":
    search_server = SearchServer('127.0.0.1', 8088)
    search_server.start()
    search_server.join()