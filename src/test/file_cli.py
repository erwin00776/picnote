import sys
import os
import struct

sys.path.append("..")
from backend.dfs.file_service import FileClient
import json
import socket
from threading import Thread


def send_one_file(filename):
    A = "127.0.0.1"
    A = "192.168.11.102"
    cli = FileClient(ip=A, port=8088)

    '''
    while True:

        line = sys.stdin.readline()
        if line is None:
            break
        print(line)
        '''
    st = os.stat(filename)
    fin = open(filename, 'rb')
    m = {'filename': filename, 'size': st.st_size, 'id': "xx"}
    j = json.dumps(m)
    print("sent %d %s" % (len(j), j))
    bs = struct.pack(">I", len(j))
    cli.send(bs)
    cli.send(j)

    buf = fin.read(4096)
    n = len(buf)
    while buf:
        # print(n)
        ok = cli.send(buf)
        if not ok:
            pass
        buf = fin.read(4096)
        n += len(buf)
    print("send done %d" % st.st_size)
    cli.close()
    '''
    bs2 = cli.recv(4)
    print(bs2)
    if bs2 is None:
        print("send failed.")
        # TODO
    recv_size = struct.unpack(">I", bs2)[0]
    if recv_size == st.st_size:
        print("send done %d" % st.st_size)
    else:
        print("send failed, %d %d" % (recv_size, st.st_size))
    '''

if __name__ == '__main__':
    thread_pool = []
    to_send_list = [
        '/home/erwin/cudnn-7.0-linux-x64-v4.0-prod.tgz',
        '/home/erwin/com.google.android.apps.photos.apk',
        '/home/erwin/Downloads/test.csv'
    ]

    for filename in to_send_list:
        t = Thread(target=send_one_file, args=(filename,))
        thread_pool.append(t)
        t.start()

    for t in thread_pool:
        t.join()
