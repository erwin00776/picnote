from base_point import BasePoint
from base_point import MachineType
from source_point import SourcePoint
from file_service import SimpleFileSrv
from store_point import StorePoint

import ctypes
import ConfigParser
import os
import json
import time
import SocketServer
from SocketServer import BaseRequestHandler
import threading
import socket
import fcntl
import struct


def get_tid():
    tid = ctypes.CDLL('libc.so.6').syscall(186)
    return tid


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])


class PeersFinderSrv(threading.Thread):
    broadcasts_port = 12345

    def __init__(self, father):
        threading.Thread.__init__(self, name="PeersFinderSrv-%d" % get_tid())
        self.is_shutdown = False
        self.heartbeat_thread = threading.Thread(target=self.heartbeat)
        self.heartbeat_thread.setName("PeersFinder-Heartbeat-%d" % get_tid())
        self.new_peers = {}
        self.peers = {}
        self.father = father
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def heartbeat(self):
        while not self.is_shutdown:
            self.peer_login()
            time.sleep(2)

    def __broadcasts(self, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(msg, ("255.255.255.255", self.broadcasts_port))

    def peer_login(self):
        self.__broadcasts("%s:%s:%f" % (self.father.uid, 'peer-login', self.father.local_last_ts))

    def peer_logout(self):
        self.__broadcasts("%s:%s:%f" % (self.father.uid, 'peer-logout', self.father.local_last_ts))

    def peer_register(self, sender, ip, last_ts):
        # print("register", sender, ip)
        self.new_peers[sender] = (ip, float(last_ts))
        self.peers[sender] = (ip, float(last_ts))

    def peer_unregister(self, sender):
        # print("unregister", sender)
        if sender in self.peers:
            del self.peers[sender]
        if sender in self.new_peers:
            del self.new_peers[sender]
        return

    def get_peers(self):
        new_peers = self.new_peers
        self.new_peers = {}
        return self.peers, new_peers

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        try:
            self.sock.bind(('', self.broadcasts_port))
        except IOError as e:
            print("error while bind %s" % e.message)
        self.peer_login()
        self.heartbeat_thread.start()
        while not self.is_shutdown:
            msg, peer = self.sock.recvfrom(1024)
            if msg is None or len(msg) < 3:
                print('empty msg from', peer)
                return
            sender, msgbody, last_ts = msg.split(':')
            if sender == self.father.uid:
                continue
            if msgbody == 'peer-login':
                print(msg)
                self.peer_register(sender, peer[0], last_ts)
            elif msgbody == 'peer-logout':
                print(msg)
                self.peer_unregister(sender)
            else:
                print("unknown msg: %s" % msg)


class MasterSyncHandler(BaseRequestHandler):
    def __init__(self, request, client_addr, server):
        BaseRequestHandler.__init__(self, request, client_addr, server)

    def handle(self):
        """
        process sync request from peer.
        1) remote peer last update timestamp
        2) body len
        3) body
        :return:
        """
        local_metas = self.server.father.local_metas
        body = json.dumps(local_metas)
        bodylen_bs = struct.pack(">I", len(body))
        self.request.send(bodylen_bs)
        self.request.send(body)


class MasterSyncSrv(SocketServer.TCPServer, threading.Thread):
    def __init__(self, srv_addr, HandleClass, father):
        SocketServer.TCPServer.__init__(self, srv_addr, HandleClass)
        threading.Thread.__init__(self, name="MasterSyncSrv-%d" % get_tid())
        self.father = father

    def run(self):
        print("## MasterSyncSrv-%s started." % get_tid())
        try:
            self.serve_forever()
        except:
            self.shutdown()


class MasterPoint(BasePoint):
    public_port = 8071
    sync_port = 8072
    op_port = 8073
    is_running = True

    def __init__(self, config_parser):
        self.config_parser = config_parser

        BasePoint.__init__(self)
        threading.Thread.__init__(self, name="MasterPoint-%d" % get_tid())

        self.uid = socket.gethostname()        # UUID
        self.store_type = MachineType.PROCESS
        self.local_ip = get_ip_address(self.config_parser.get('base', 'interface'))

        source_path = self.config_parser.get('base', 'source_path')
        roots = source_path.split(':')
        self.source_point = SourcePoint(roots)       # local / source point
        self.store_point = StorePoint('/home/erwin/store_tmp')  # store point

        self.peers = {}
        self.metas = {}                         # all peers meta.
        self.local_metas = {}
        self.local_last_ts = 0  # last local update timestamp

        self.peer_svr = PeersFinderSrv(self)
        self.peer_svr.start()
        self.sync_svr = MasterSyncSrv((self.local_ip, self.sync_port), MasterSyncHandler, self)
        self.sync_svr.start()
        self.file_srv = SimpleFileSrv()
        self.file_srv.start()

        self.__last_times = {}

    def __check_last_time(self, func_name, interval=600):
        t = time.time()
        if func_name in self.__last_times:
            if (t - self.__last_times[func_name]) > interval:
                return True, self.__last_times[func_name]
            else:
                return False, self.__last_times[func_name]
        return True, None

    def __update_last_time(self, func_name):
        t = time.time()
        self.__last_times[func_name] = t

    def shutdown(self):
        self.is_running = False

    def scan_self_store(self):
        check, ts = self.__check_last_time('scan_self_store')
        if not check:
            return
        self.__update_last_time('scan_self_store')

    def handle_diff(self, peer_ip, addfiles, delfiles):
        print("sync from %s" % peer_ip)
        print("\tadd: ", addfiles)
        for (f, vals) in addfiles.items():
            print(f, vals)
            self.store_point.store('', peer_ip, f, vals)
        print("\tdel: ", delfiles)
        for (f, vals) in delfiles.items():
            self.store_point.remove(f)

    def sync_one(self, peer_name, peer_ip):
        """
        sync with one peer.
        :param peer_name: peer name
        :param peer_ip: peer (ip, port)
        :return:
        """
        print("start sync with %s %s" % (peer_name, peer_ip))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((peer_ip, self.sync_port))

            # check last update timestamp
            local_meta = self.metas.get(peer_name, None)
            # start sync meta info
            bs = sock.recv(4)
            body_len = struct.unpack(">I", bs)[0]
            body = sock.recv(body_len)
            remote_meta = json.loads(body)

            add_files = {}
            del_files = {}
            mv_files = {}           # TODO find by md5sum?
            if peer_name in self.metas:
                remote_copy = remote_meta.copy()
                for fn, vals in local_meta.items():
                    if fn in remote_meta:
                        del remote_copy[fn]
                    else:
                        del_files[fn] = vals
                add_files = remote_copy
            else:
                add_files = remote_meta
            self.metas[peer_name] = remote_meta
            print("recv remote data: ", remote_meta)

            # handle differences
            self.handle_diff(peer_ip, add_files, del_files)
        except IOError as e:
            print("sync error: %s" % e.message)

    def check_peers(self, peers):
        """ handle diff for every peers"""
        for (peer_name, peer_val) in peers.items():
            peer_ip, peer_ts = peer_val
            local_peer = self.peers.get(peer_name, (0, 0))
            if peer_ts > local_peer[1]:
                self.sync_one(peer_name, peer_ip)
        self.peers = peers.copy()

    def run(self):
        print("## main: %d" % get_tid())
        try:
            while self.is_running:
                self.local_last_ts, self.local_metas = self.source_point.get_files_meta()
                if self.local_last_ts is None:
                    self.local_last_ts = 0
                    self.local_metas = {}
                # self.local_metas['last_ts'] = self.local_last_ts
                self.metas[self.uid] = self.local_metas

                peers, new_peers = self.peer_svr.get_peers()    # {peer_name: (peer_ip, ts)}
                self.check_peers(peers)

                time.sleep(2)
        except IOError as e:
            print(e.message)

if __name__ == '__main__':
    config_parser = None
    if os.path.exists('simple_dfs.cfg'):
        config_parser = ConfigParser.SafeConfigParser()
        config_parser.read('simple_dfs.cfg')
    mpoint = MasterPoint(config_parser)
    mpoint.start()
    mpoint.join()