# coding=utf-8
import SocketServer
import socket
import BaseHTTPServer
from src.backend.utils.superior_thread import SuperiorThread


class StatusHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def __init__(self, request, cli_addr, server):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, cli_addr, server)

    def do_GET(self):
        if self.server.master:
            store_status = self.server.master.get_store_status()
            buf = "<b>Store Status</b><br/>"
            for md5id, val in store_status.items():
                buf += "%s %s<br/>" % (md5id, str(val))
            buf += "</br></br>"
            # buf = "<b>Store Status</b> <br/> %s <br/><br/>" % str(store_status)
            peers_status = self.server.master.get_peers_status()
            if len(peers_status) == 0:
                buf += "<b>(No others peers!</b><br/>"
            for peer_name, peer_val in peers_status.items():
                # buf += "<b>%s</b>%s<br/><br/>" % (peer_name, str(peer_val))
                buf += "<b>%s</b><br/>" % peer_name
                peer_metas = self.server.master.get_metas(peer_name)
                if peer_metas is None:
                    buf += "(None)<br/>"
                    continue
                for md5id, val in peer_metas.items():
                    buf += "%s %s<br/>" % (md5id, str(val))
                buf += "<br/><br/>"
        else:
            buf = "server master not found."
        self.protocal_version = "HTTP / 1.1"
        self.send_response(200)
        self.send_header("Welcome", "Contect")
        self.end_headers()
        self.wfile.write(buf)


class StatusServer(SocketServer.TCPServer, SuperiorThread):
    allow_reuse_address = 1  # Seems to make sense in testing environment

    def __init__(self, master=None, ip="127.0.0.1", port=8011):
        SocketServer.TCPServer.__init__(self, (ip, port), StatusHandler, bind_and_activate=True)
        SuperiorThread.__init__(self, daemon=True, name="StatusServer")
        self.master = master
        self.ip = ip
        self.port = port

    def server_bind(self):
        """Override server_bind to store the server name."""
        SocketServer.TCPServer.server_bind(self)
        host, port = self.socket.getsockname()[:2]
        self.server_name = socket.getfqdn(host)
        self.server_port = port

    def run(self):
        self.serve_forever()


if __name__ == "__main__":
    status_srv = StatusServer()
    status_srv.start()