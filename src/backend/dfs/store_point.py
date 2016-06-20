from base_point import BasePoint
from base_point import MachineType


class DummyStorePoint(BasePoint):
    def __init__(self):
        self.store_type = MachineType.STORE
        BasePoint.__init__(self)


class StorePoint(BasePoint):

    def __init__(self):
        BasePoint.__init__(self)
        self.store_type = MachineType.STORE
        self.name = ""

    def get_name(self, root):
        pass