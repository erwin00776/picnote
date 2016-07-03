import os
import hashlib


def fileid(path):
    if not os.path.exists(path):
        return None
    f = open(path, 'rb')
    m = hashlib.md5()
    buf = f.read(4096)
    while buf is not None:
        m.update(buf)
        buf = f.read(4096)
    h = m.hexdigest()
    return h
