from base_point import BasePoint
from base_point import MachineType
from source_point import SourcePoint
import socket
import struct
import json
import time
import SocketServer
import threading
from SocketServer import BaseRequestHandler

import socket
import fcntl
import struct


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])


class FindingHandler(BaseRequestHandler):                # answer the remote peers.
    def __init__(self, request, client_addr, server):
        BaseRequestHandler.__init__(self, request, client_addr, server)

    def handle(self):
        # server add peer
        remote_name = self.request.recv(128)
        remote_name = remote_name.strip()
        print("remote peer recv: %s" % remote_name)
        self.server.add_peer(remote_name, self.client_address)
        self.request.send(self.server.name)

    def finish(self):
        self.request.close()


class FindingSrv(SocketServer.TCPServer, threading.Thread):
    is_running = True

    def __init__(self, srv_addr, HandleClass, name, father):
        self.allow_reuse_address = True
        SocketServer.TCPServer.__init__(self, srv_addr, HandleClass)
        threading.Thread.__init__(self, name="FindingSrv")
        self.name = name
        self.peers = {}
        self.new_peers = {}
        self.last_times = {}
        self.father = father
        self.scan_thread = threading.Thread(target=self.auto_scan_peers, name="auto-scanner")

    def shutdown(self):
        self.is_running = False

    def __check_last_time(self, func_name, interval=600):
        t = time.time()
        if func_name in self.last_times:
            if (t - self.last_times[func_name]) > interval:
                return True, self.last_times[func_name]
            else:
                return False, self.last_times[func_name]
        return True, None

    def __update_last_time(self, func_name):
        t = time.time()
        self.last_times[func_name] = t

    def run(self):
        self.scan_thread.start()
        try:
            self.serve_forever()
        except:
            print("serve failed.")

    def get_connected_peers(self):
        """
        :return: all_peers, new_peers
        """
        return self.peers, self.new_peers

    def add_peer(self, name, ip):
        if name in self.peers:
            return
        self.peers[name] = ip
        print(name, ip)

    def auto_scan_peers(self):
        while self.is_running:
            print('scanning peers')
            self.scan_peers('192.168.11.', (102, 104))
            time.sleep(2)

    def scan_peers(self, ip_prefix, ip_range):
        check, ts = self.__check_last_time('scan_peers', interval=3)
        if not check:
            return

        peers = {}
        new_peers = {}
        for i in range(ip_range[0], ip_range[1]):
            ip = ip_prefix + str(i)
            if self.father.local_ip == ip:
                continue
            print("scan peer trying: %s" % ip)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((ip, self.father.public_port))
            except IOError as e:
                print(e.message)
                continue

            sock.send(self.name)
            peer_name = sock.recv(128)
            peer_name = peer_name.strip()
            peers[peer_name] = ip
            if peer_name not in self.peers:
                new_peers[peer_name] = ip
        print('all peers: ', peers)
        print('new peers: ', new_peers)
        self.peers = peers
        self.new_peers = new_peers
        self.__update_last_time('scan_peers')


class PeersFinderHandler(BaseRequestHandler):
    def __init__(self, request, client, server):
        BaseRequestHandler.__init__(self, request, client, server)

    def handle(self):
        pass


class PeersFinderSrv(SocketServer.TCPServer, threading.Thread):
    def __init__(self, srv_addr, HandleClass):
        SocketServer.TCPServer.__init__(self, srv_addr, HandleClass)
        threading.Thread.__init__(self, name="PeersFinderSrv")

    def run(self):
        try:
            self.serve_forever()
        except IOError as e:
            print(e.message)


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
        print("sync from %s" % str(self.client_address))    # get/send peer ts in local
        local_ts = self.server.father.local_last_ts
        local_ts_bs = struct.pack(">I", local_ts)
        self.request.send(local_ts_bs)

        remote_ts_bs = self.request.recv(4)                 # get and compare remote ts
        remote_ts = struct.unpack(">I", remote_ts_bs)[0]
        if local_ts == remote_ts:
            return

        local_metas = self.server.father.local_metas
        body = json.dumps(local_metas)
        bodylen_bs = struct.pack(">I", len(body))
        self.request.send(bodylen_bs)
        self.request.send(body)


class MasterSyncSrv(SocketServer.TCPServer, threading.Thread):
    def __init__(self, srv_addr, HandleClass, father):
        SocketServer.TCPServer.__init__(self, srv_addr, HandleClass)
        threading.Thread.__init__(self, name="MasterSyncSrv")
        self.father = father

    def run(self):
        print("MasterSyncSrv-%s started." % self.getName())
        try:
            self.serve_forever()
        except:
            self.shutdown()


class MasterPoint(BasePoint):
    public_port = 8071
    sync_port = 8072
    op_port = 8073
    is_running = True

    def __init__(self):
        BasePoint.__init__(self)
        threading.Thread.__init__(self, name="MasterPoint")
        self.store_type = MachineType.PROCESS
        self.source_point = SourcePoint()       # local/ source point
        self.local_last_ts = 0                  # last local update timestamp
        self.store_point = None                 # store point
        self.uid = socket.gethostname()        # UUID
        self.peers = {}
        self.metas = {}                         # all peers meta.
        self.local_metas = {}

        self.local_ip = get_ip_address('eth0')
        self.finding_svr = FindingSrv((self.local_ip, self.public_port), FindingHandler, self.name, self)
        self.finding_svr.start()
        self.sync_svr = MasterSyncSrv((self.local_ip, self.sync_port), MasterSyncHandler, self)
        self.sync_svr.start()

        self.last_times = {}

    def __check_last_time(self, func_name, interval=600):
        t = time.time()
        if func_name in self.last_times:
            if (t - self.last_times[func_name]) > interval:
                return True, self.last_times[func_name]
            else:
                return False, self.last_times[func_name]
        return True, None

    def __update_last_time(self, func_name):
        t = time.time()
        self.last_times[func_name] = t

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
        print("\tdel: ", delfiles)

    def sync_one(self, peer, peer_ip):
        """
        sync with one peer.
        :param peer: peer name
        :param peer_ip: peer (ip, port)
        :return:
        """
        # print("start sync with %s %s" % (peer, peer_ip))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((peer_ip, self.sync_port))

            # check last update timestamp
            ts_bs = sock.recv(4)
            if ts_bs is None or len(ts_bs) != 4:
                print("error in sync ts", ts_bs)
            remote_ts = struct.unpack(">I", ts_bs)[0]
            local_meta = self.metas.get(peer, None)
            local_ts = 0
            if local_meta is not None:
                local_ts = local_meta['last_ts']
            ts_bs = struct.pack(">I", local_ts)
            sock.send(ts_bs)

            print("remote ts: %s, local ts: %s" % (remote_ts, local_ts))

            if local_ts == remote_ts:
                # ignore this peer for no updates
                return

            # start sync meta info
            bs = sock.recv(4)
            body_len = struct.unpack(">I", bs)[0]
            body = sock.recv(body_len)

            remote_meta = json.loads(body)

            addfiles = {}
            delfiles = {}
            if peer in self.metas:
                remote_copy = remote_meta.copy()
                for fn, vals in local_meta.items():
                    if fn in remote_meta:
                        del remote_copy[fn]
                    else:
                        delfiles[fn] = vals
                addfiles = remote_copy
            else:
                addfiles = remote_meta
#            remote_meta['last_ts'] = remote_ts
            self.metas[peer] = remote_meta
            print("recv remote data: ", remote_meta)

            # handle differences
            self.handle_diff(peer_ip, addfiles, delfiles)
        except IOError as e:
            print("sync error: %s" % e.message)


    def sync_all(self, remotes):
        pass

    def run(self):
        try:
            while self.is_running:
                self.scan_self_store()
                self.local_last_ts, self.local_metas = self.source_point.get_files_meta()
                self.local_metas['last_ts'] = self.local_last_ts
                self.metas[self.uid] = self.local_metas

                all_peers, new_peers = self.finding_svr.get_connected_peers()
                for (peer, ip) in new_peers.items():
                    self.sync_one(peer, ip)
        except IOError as e:
            print(e.message)

if __name__ == '__main__':
    mpoint = MasterPoint()
    mpoint.start()
    mpoint.join()