from base_point import BasePoint
from base_point import MachineType
from source_point import SourcePoints
from file_service import SimpleFileSrv
from store_point import StorePoints

import redis
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


class FID:
    def __init__(self, purpose_id, lid, rid):
        self.purpose_id = purpose_id
        self.lid = lid
        self.rid = rid
        self.fids = []

    def add(self, fid):
        self.fids.append(fid)

    def check(self):
        return len(self.fids)


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
        self.cur_fid = -1
        self.next_fid = -1
        self.fid_set = {}
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

    def fid_alloc1(self, porpose_id, lid, rid):
        self.__broadcasts("%s:fid-alloc1:%d:%d:%d" % (self.father.uid, porpose_id, lid, rid))

    def fid_alloc2(self, porpose_id, lid, rid):
        self.__broadcasts("%s:fid-alloc2:%d:%d:%d" % (self.father.uid, porpose_id, lid, rid))

    def fid_reply(self, purpose_id, ans):
        self.__broadcasts("%s:fid-reply:%d:%d" % (self.father.uid, purpose_id, ans))

    def peer_register(self, sender, ip, last_ts):
        self.new_peers[sender] = (ip, float(last_ts))
        self.peers[sender] = (ip, float(last_ts))

    def peer_unregister(self, sender):
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

    def fid_alloc(self, size):
        if size < 1:
            return True, 0
        if len(self.peers) == 0:
            self.cur_fid = 0
            self.next_fid = self.cur_fid + 1
            return True, self.cur_fid
        purpose_id = int(time.time() * 1000000 % 10000000000000)
        got = False
        while not got:
            lid, rid = self.cur_fid, self.cur_fid + size
            self.fid_alloc1(purpose_id, lid, rid)
            wait_time = 0
            while wait_time < 10:
                fid = self.fid_set.get(purpose_id, None)
                if fid is not None and len(fid.fids) == len(self.peers):
                    check = True
                    for remote_fid in fid.fids:
                        if remote_fid > lid:
                            check = False
                            break
                    got = check
                    if got:
                        break
                wait_time += 1
                time.sleep(1)
        if got:
            self.fid_alloc2(purpose_id, lid, rid)
            self.cur_fid = rid + 1
            self.next_fid = self.cur_fid + 1
            return True, lid
        return False, -1

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
            pieces = msg.split(':')
            sender, msgbody = pieces[0], pieces[1]
            if sender == self.father.uid:
                continue

            if msgbody == 'peer-login':
                print(msg)
                last_ts = pieces[2]
                self.peer_register(sender, peer[0], last_ts)
            elif msgbody == 'peer-logout':
                print(msg)
                self.peer_unregister(sender)
            elif msgbody == 'fid-alloc1':
                purpose_id, lid, rid = int(pieces[2]), int(pieces[3]), int(pieces[4])
                self.fid_reply(purpose_id, self.cur_fid)
            elif msgbody == 'fid-alloc2':
                purpose_id, lid, rid = int(pieces[2]), int(pieces[3]), int(pieces[4])
                self.next_fid = rid + 1
            elif msgbody == 'fid-reply':
                purpose_id, fid = int(pieces[2]), int(pieces[3])
                replys = self.fid_set.get(purpose_id, FID())
                replys.add(fid)
                self.fid_set[purpose_id] = replys
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
        self.allow_reuse_address = True
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
        self.redis_cli = redis.Redis()

        BasePoint.__init__(self)
        threading.Thread.__init__(self, name="MasterPoint-%d" % get_tid())

        self.uid = socket.gethostname()        # UUID
        self.store_type = MachineType.PROCESS
        self.local_ip = get_ip_address(self.config_parser.get('base', 'interface'))

        sources_path = self.config_parser.get('base', 'sources_path')
        stores_path = self.config_parser.get('base', 'stores_path')

        roots = sources_path.split(':')
        self.source_points = SourcePoints(roots, self.redis_cli)       # local / source point
        self.source_points.start()

        store_roots = stores_path.split(':')            #['/home/erwin/store_tmp']
        self.store_points = StorePoints(store_roots)  # store point
        self.store_points.start()

        self.peers = {}
        self.metas = {}                         # all peers meta.
        self.local_metas = {}
        self.local_last_ts = 0  # last local update timestamp

        self.peer_svr = PeersFinderSrv(self)
        self.peer_svr.start()
        self.sync_svr = MasterSyncSrv((self.local_ip, self.sync_port), MasterSyncHandler, self)
        self.sync_svr.start()
        self.file_srv = SimpleFileSrv(self.local_ip, self.op_port)
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
            self.store_points.store('', peer_ip, f, vals)
        print("\tdel: ", delfiles)
        for (f, vals) in delfiles.items():
            self.store_points.remove(f)

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
        except ValueError as e:
            print("sync error: %s, body: %s" % (e.message, body))

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
                self.local_last_ts, self.local_metas, add_files, del_files = self.source_points.get_metas()
                ret, fid = self.peer_svr.fid_alloc(len(add_files))
                while not ret:
                    ret, fid = self.peer_svr.fid_alloc(len(add_files))
                for k, v in add_files.items():
                    v['fid'] = fid
                    fid += 1
                    self.local_metas[k] = v
                if self.local_last_ts is None:
                    self.local_last_ts = 0
                    self.local_metas = {}
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