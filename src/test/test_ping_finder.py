import sys
sys.path.append("..")
from backend.dfs.master_point import FindingSrv
from backend.dfs.master_point import FindingHandler

if __name__ == '__main__':
    srv = FindingSrv(('192.168.11.103', 8071), FindingHandler, 'test-finder')
    srv.start()
    srv.join()