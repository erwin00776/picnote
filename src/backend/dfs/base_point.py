import threading

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

MachineType = Enum(['SOURCE', 'STORE', 'PROCESS'])


class BasePoint(threading.Thread):
    store_type = MachineType.SOURCE

    def __init__(self):
        pass

    def run(self):
        pass

    def serv(self):
        pass

if __name__ == '__main__':
    type = MachineType.SOURCE
    print(type)