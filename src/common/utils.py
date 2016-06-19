
import struct

def h2n_bytes(x):
    bs = struct.pack(">I", len(x))
    return bs

def n2h_bytes(bs):
    header_len = struct.unpack(">I", bs)[0]