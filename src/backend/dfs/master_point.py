import ConfigParser
import SocketServer
import ctypes
import fcntl
import json
import os
import socket
import struct
import time
from SocketServer import BaseRequestHandler

import redis

import sys
if len(sys.argv) > 1:
    print("adding search path: %s" % sys.argv[1])
    sys.path.append(sys.argv[1])

from base_point import BasePoint
from base_point import MachineType
from source_point import SourcePoints
from file_service import SimpleFileSrv
from status_server import StatusServer
from store_point import StorePoints
from src.backend.utils.dfs_log import LOG
from src.backend.utils.superior_thread import SuperiorThread


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


class PeersFinderSrv(SuperiorThread):
    broadcasts_port = 12345

    def __init__(self, father):
        SuperiorThread.__init__(self, daemon=True, name="PeersFinderSrv-%d" % get_tid(), critical=True)
        self.is_shutdown = False
        self.heartbeat_interval = 15        # seconds
        self.heartbeat_thread = SuperiorThread(target=self.heartbeat,
                                               name="PeersFinder-Heartbeat-%d" % get_tid(),
                                               critical=True)
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
            time.sleep(self.heartbeat_interval)

    def __broadcasts(self, msg):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(msg, ("255.255.255.255", self.broadcasts_port))

    def peer_login(self):
        self.__broadcasts("%s:%s:%f" % (self.father.uid, 'peer-login', self.father.local_last_ts))

    def peer_logout(self):
        self.__broadcasts("%s:%s:%f" % (self.father.uid, 'peer-logout', self.father.local_last_ts))

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
        """ {peer_name: (ip, last_ts)} """
        new_peers = self.new_peers
        self.new_peers = {}
        return self.peers, new_peers

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        try:
            self.sock.bind(('', self.broadcasts_port))
        except IOError as e:
            LOG.info("error while bind %s" % e.message)
        self.peer_login()
        self.heartbeat_thread.start()

        while not self.is_shutdown:
            msg, peer = self.sock.recvfrom(1024)
            if msg is None or len(msg) < 3:
                LOG.debug('empty msg from %s' % peer)
                return
            pieces = msg.split(':')
            sender, msgbody = pieces[0], pieces[1]
            if sender == self.father.uid:
                continue

            if msgbody == 'peer-login':
                LOG.debug(msg)
                last_ts = pieces[2]
                self.peer_register(sender, peer[0], last_ts)
            elif msgbody == 'peer-logout':
                LOG.debug(msg)
                self.peer_unregister(sender)
            else:
                LOG.debug("unknown msg: %s" % msg)


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


class MasterSyncSrv(SocketServer.TCPServer, SuperiorThread):
    def __init__(self, srv_addr, HandleClass, father):
        self.allow_reuse_address = True
        SocketServer.TCPServer.__init__(self, srv_addr, HandleClass)
        # threading.Thread.__init__(self, name="MasterSyncSrv-%d" % get_tid())
        SuperiorThread.__init__(self, name="MasterSyncSrv-%d" % get_tid())
        self.father = father

    def crash(self):
        pass

    def run(self):
        while not self.father.inited:
            time.sleep(1)

        try:
            self.serve_forever()
        except:
            self.shutdown()


def meta_diff(m1, m2, detail_cmp=False):
    """
    :param m1: compare hash
    :param m2: base meta hash
    :return: added/deleted
    """
    added, deleted = {}, m2.copy()
    for md5id, val in m1.items():
        if md5id not in m2:
            added[md5id] = val
        elif detail_cmp and (m1[md5id]['mtime'] != m2[md5id]['mtime'] or
                             m1[md5id]['size'] != m2[md5id]['size']):
            added[md5id] = val
        else:
            del deleted[md5id]
    return added, deleted


class MasterPoint(BasePoint):
    public_port = 8071
    sync_port = 8072
    op_port = 8073
    is_running = True

    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.redis_cli = redis.Redis()
        self.inited = False

        BasePoint.__init__(self)
        # threading.Thread.__init__(self, name="MasterPoint-%d" % get_tid())
        SuperiorThread.__init__(self, name="MasterPoint-%d"%get_tid(), daemon=True, critical=True)

        self.uid = socket.gethostname()        # UUID
        self.store_type = MachineType.PROCESS
        self.local_ip = get_ip_address(self.config_parser.get('base', 'interface'))
        status_port = int(self.config_parser.get('server', 'status_port'))
        self.status_server = StatusServer(master=self, ip=self.local_ip, port=status_port)

        sources_path = self.config_parser.get('base', 'sources_path')
        stores_path = self.config_parser.get('base', 'stores_path')

        roots = sources_path.split(':')
        self.source_points = SourcePoints(roots, self.redis_cli)       # local / source point
        self.source_points.start()

        store_roots = stores_path.split(':')
        self.store_points = StorePoints(store_roots, self.redis_cli)    # store point
        self.store_points.start()

        self.peers = {}                                 # {peer_name: {val}}
        self.metas = {}                                 # {peer_name: {val}}
        self.local_metas = {}
        self.local_last_ts = 0                          # last local update timestamp

        self.peer_svr = PeersFinderSrv(self)
        self.peer_svr.start()
        self.sync_svr = MasterSyncSrv((self.local_ip, self.sync_port), MasterSyncHandler, self)
        self.sync_svr.start()
        self.file_srv = SimpleFileSrv(self.local_ip, self.op_port)
        self.file_srv.start()
        self.__last_times = {}

    def get_store_status(self):
        return self.metas[self.uid]

    def get_source_status(self):
        return self.peers

    def get_peers_status(self):
        return self.peers

    def crash(self):
        pass

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

    def handle_from_remote(self, peer_ip, add_files, del_files):
        # LOG.debug("sync from %s" % peer_ip)
        LOG.debug("$$$ %s\n" % str(add_files))
        for (f, val) in add_files.items():
            self.store_points.store('', peer_ip, f, val)
        for (f, val) in del_files.items():
            self.store_points.remove(f)

    def handle_from_local(self, add_files, del_files):
        for md5id, val in add_files.items():
            self.store_points.store(None, None, val['md5id'], val)
        for md5id, val in del_files.items():
            # TODO handle deleted file: logical del from redis?
            pass

    def sync_one(self, peer_name, peer_ip):
        """
        sync with one peer.
        :param peer_name: peer name
        :param peer_ip: peer (ip, port)
        :return:
        """
        LOG.info("start sync with %s %s" % (peer_name, peer_ip))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((peer_ip, self.sync_port))

            # check last update timestamp
            local_meta = self.metas.get(peer_name, None)
            # start sync meta info
            bs = sock.recv(4)
            body_len = struct.unpack(">I", bs)[0]
            body = sock.recv(body_len)
            while len(body) < body_len:
                body_extra = sock.recv(body_len - len(body))
                body += body_extra

            remote_meta = json.loads(body)
            LOG.debug("remote_meta %s" % str(remote_meta))

            add_files = {}
            del_files = {}
            mv_files = {}           # TODO find by md5sum?
            if peer_name in self.metas:
                add_files, del_files = meta_diff(remote_meta, self.local_metas)
            else:
                add_files = remote_meta
            self.metas[peer_name] = remote_meta

            self.handle_from_remote(peer_ip, add_files, del_files)
        except ValueError as e:
            LOG.debug("sync error: %s, body: %s" % (e.message, body))

    def get_metas(self, peer_name):
        return self.metas.get(peer_name, None)

    def check_peers(self, peers):
        """ handle diff for every peers"""
        # compare remote files.
        for (peer_name, peer_val) in peers.items():
            if peer_name == self.uid:
                continue
            peer_ip, peer_ts = peer_val
            prev_status = self.peers.get(peer_name, (0, 0))
            LOG.debug("peer %s cur:%d prev:%d" % (peer_name, peer_ts, prev_status[1]))
            if peer_ts > prev_status[1]:
                self.sync_one(peer_name, peer_ip)
        self.peers = peers.copy()

    def check_sources(self):
        # compare local s.
        src_last_ts, source_metas, src_added, src_deleted = self.source_points.get_metas()
        soe_last_ts, store_metas = self.store_points.get_metas()
        add_files, del_files = meta_diff(source_metas, store_metas, detail_cmp=True)
        if len(add_files) > 0:
            LOG.info("check sources found: %d" % len(add_files))
        self.handle_from_local(add_files, del_files)
        self.local_last_ts = src_last_ts if src_last_ts > soe_last_ts else soe_last_ts
        return source_metas, store_metas

    def dump_infos(self):
        meta_key = "$metas.%s" % self.uid
        peer_key = "$peers.%s" % self.uid
        for peer_name, val in self.metas.items():
            s = json.dumps(val)
            self.redis_cli.hset(meta_key, peer_name, s)
        for peer_name, val in self.peers.items():
            s = json.dumps(val)
            self.redis_cli.hset(peer_key, peer_name, s)

    def load_infos(self):
        meta_key = "$metas.%s" % self.uid
        peer_key = "$peers.%s" % self.uid
        metas = self.redis_cli.hgetall(meta_key)
        peers = self.redis_cli.hgetall(peer_key)
        if not metas:
            self.metas = {}
        if not peers:
            self.peers = {}
        for peer_name, peer_val in metas.items():
            val = json.loads(peer_val)
            self.metas[peer_name] = val

    def run(self):
        # wait for local resources loaded.
        self.status_server.start()
        self.load_infos()

        self.inited = False
        while not self.source_points.wait4init():
            LOG.info("waiting for source inited.")
            time.sleep(1)
        while not self.store_points.wait4init():
            LOG.info("waiting for store inited.")
            time.sleep(1)
        self.inited = True

        # process local sources when started.
        source_metas, store_metas = self.check_sources()
        LOG.info("#1(sources) %s" % str(source_metas))
        LOG.info("#2(stores) %s" % str(store_metas))

        self.is_running = True

        try:
            while self.is_running:
                """ rescan local sources """
                source_metas, store_metas = self.check_sources()

                """ merge source's metas and store's. """
                # TODO peer meta: store_meta? source+store?
                self.metas[self.uid] = store_metas
                self.local_metas = store_metas

                """ process remote peers changes """
                peers, new_peers = self.peer_svr.get_peers()    # {peer_name: (peer_ip, ts)}
                self.check_peers(peers)
                if new_peers and len(new_peers) > 0:
                    self.dump_infos()

                time.sleep(23)
        except IOError as e:
            LOG.error("master io error: ", e.message)


if __name__ == '__main__':
    config_parser = None

    config_path = "simple_dfs.cfg"
    if len(sys.argv) > 2:
        config_path = sys.argv[2]
    if os.path.exists(config_path):
        config_parser = ConfigParser.SafeConfigParser()
        config_parser.read(config_path)
    master_point = MasterPoint(config_parser)
    master_point.start()
    master_point.join()
