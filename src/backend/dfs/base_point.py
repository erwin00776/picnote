import threading
import sys
from src.backend.utils.superior_thread import SuperiorThread
from src.backend.utils.dfs_log import LOG


class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

MachineType = Enum(['SOURCE', 'STORE', 'PROCESS'])
StoreLevel = Enum(['One', 'Two', 'Three', 'Four'])


class BasePoint(SuperiorThread):
    store_type = MachineType.SOURCE
    store_level = StoreLevel.Three

    def __init__(self):
        SuperiorThread.__init__(self, daemon=True)

    def crash(self):
        #raise NotImplementedError('BasePoint crash() not implement yet.')
        SuperiorThread.crash(self)

    def run(self):
        pass

    def serv(self):
        pass

if __name__ == '__main__':
    type = MachineType.SOURCE
    print(type)