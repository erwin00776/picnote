import os
import threading
import socket
import SocketServer
from seqfile import SeqFile


class MasterHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.server = server
        SocketServer.BaseRequestHandler.__init__(request, client_address, server)

    def handle(self):
        self.data = self.request.recv(1024).strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        self.request.sendall(self.data.upper())


class BMaster(SocketServer.TCPServer, threading.Thread):
    def __init__(self, server_address, RequestHandlerClass):
        self.logfile = SeqFile()
        self.start_server()
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)

    def start_server(self):
        # 1) connect to redis
        # 2) open port
        # 3)
        pass

    def server_close(self):
        SocketServer.TCPServer.server_close(self)


def client(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    try:
        sock.sendall(message)
        response = sock.recv(1024)
        print "Received: {}".format(response)
    finally:
        sock.close()

if __name__ == "__main__":
    HOST, PORT = "localhost", 8019

    server = BMaster((HOST, PORT), MasterHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name

    client(ip, port, "Hello World 1")
    client(ip, port, "Hello World 2")
    client(ip, port, "Hello World 3")

    server.shutdown()
    server.server_close()