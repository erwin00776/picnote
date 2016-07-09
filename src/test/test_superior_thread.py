import threading
import time


class __SuperiorDaemon(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.pool = []
        self.is_shutdown = False

    def shutdown(self):
        self.is_shutdown = True

    def run(self):
        self.is_shutdown = False
        while not self.is_shutdown:
            for t in self.pool:
                if not t.isAlive():
                    t.crash()
            time.sleep(1)

    def add(self, t):
        self.pool.append(t)

_superior_daemon = __SuperiorDaemon()
_superior_daemon.start()


class SuperiorThread(threading.Thread):
    def __init__(self, daemon=False, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs, verbose)
        self.daemon = daemon

    def crash(self):
        print("%s crashed." % self.name)

    def run(self):
        _superior_daemon.add(self)
        threading.Thread.run(self)


def says():
    i = 0
    while True:
        i += 1
        if i > 15:
            raise Exception("crash down!")
        print(i)
        time.sleep(1)

if __name__ == "__main__":
    t = SuperiorThread(daemon=True, target=says)
    t.start()
    t.join()