import os
import sys

class DuSpace:
    def __init__(self):
        self.FILESIZE_WATERLINE = 512*1024*1024
        self.DIRSIZE_WATERLINE = 2 * 1024 * 1024 * 1024

    def getsize(self, root, fname):
        fpath = os.path.join(root, fname)
        if not os.path.exists(fpath):
            return 0
        st = os.stat(fpath)
        return st.st_size

    def getsizes(self, root, files):
        filesizes = [self.getsize(root, f) for f in files]
        bigfiles = {}
        for i in range(len(files)):
            if filesizes[i] > self.FILESIZE_WATERLINE:
                bigfiles[files[i]] = filesizes[i]
        return filesizes, bigfiles

    def calc_count(self, path):
        for root, dirs, files in os.walk(path):
            filesizes, bigfiles = self.getsizes(root, files)
            dirsize = sum(filesizes)
            dirsize_mb = dirsize / 1024 / 1024
            if dirsize > self.DIRSIZE_WATERLINE:
                print("[D] %d(MB) %s" % (dirsize_mb, root))
            for (k, v) in bigfiles.items():
                print("[F] %d(MB) %s" % (v/1024/1024, os.path.join(root, k)))

if __name__ == '__main__':
    ds = DuSpace()
    ds.calc_count("/home/erwin")

