import threading
import time
from dfs_log import LOG


class __SuperiorDaemon(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.pool = []
        self.is_shutdown = False

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        LOG.debug("superior monitor started.")
        self.is_shutdown = False
        while not self.is_shutdown:
            alives = []
            for t in self.pool:
                if not t.isAlive():
                    t.crash()
                else:
                    alives.append(t)
            self.pool = alives
            time.sleep(1)

    def add(self, t):
        self.pool.append(t)

_superior_daemon = __SuperiorDaemon()
_superior_daemon.start()


class SuperiorThread(threading.Thread):
    def __init__(self, daemon=True, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs, verbose)
        self.daemon = daemon
        if self.daemon:
            _superior_daemon.add(self)

    def crash(self):
        LOG.debug("%s crashed." % self.name)

    def run(self):
        threading.Thread.run(self)
