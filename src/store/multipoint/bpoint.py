import os
import hashlib
import shutil
from seqfile import SeqFile


class BPoint:
    def __init__(self, name):
        self.name = name
        self.basepath = ''
        self.seqfile = SeqFile()
        m = hashlib.md5()

    def get_file_id(self, src_path):
        f = open(src_path, 'r')
        data = f.read(-1)
        self.m.update(data)
        return self.m.hexdigest()

    def genpath(self, src_path):
        src_name = os.path.basename(src_path)

        self.m.update(src_name)
        digest = self.m.hexdigest()
        dest_path = "%s/%s/%s" % (self.basepath, str(digest[:4], src_name))
        return dest_path

    def add_one(self, src_path):
        dest_path = self.genpath(src_path)
        shutil.copy(src_path, dest_path)

        os.rename(src_path, dest_path)


    def del_one(self, filepath):
        dest_path = self.genpath(filepath)


if __name__ == "__main__":
    bpoint = BPoint()
    bpoint.run()