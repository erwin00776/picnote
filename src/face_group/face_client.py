import socket
import redis
import struct

class FaceClient:
    def __init__(self):
        self.address = ('127.0.0.1', 9999)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.address)
        self.redis_cli = redis.Redis()

    def send_feat_request(self, image_path):
        self.sock.send("feat" + struct.pack(">I", len(image_path)))
        self.sock.send(image_path)
        header = self.sock.recv(32)
        if header is None or len(header) == 0:
            return -1
        header = header.strip()
        face_count, feat_len = header.split(':')
        face_count = int(face_count)
        feat_len = int(feat_len)
        print("%d %d" % (face_count, feat_len))
        face_features = []
        for face_id in range(face_count):
            features = self.sock.recv(20480)
            if features is None or len(features) == 0:
                print("skip bad features at %d" % face_id)
                continue
            self.handle_features(image_path, face_id, features)
            face_features.append((face_id, features))
        return face_features

    def send_dist_request(self, feat1, feat2):
        self.sock.send("dist")

        len1 = struct.pack(">I", len(feat1))
        len2 = struct.pack(">I", len(feat2))
        print(len(len1), len1, len2, len(feat1), len(feat2))
        self.sock.send(len1 + len2)

        self.sock.send(feat1)
        self.sock.send(feat2)
        simility = self.sock.recv(32)
        return simility

    def handle_features(self, image_path, face_id, features):
        print(image_path)
        print(face_id)
        # print(features)

    def start(self, dir_path):
        feat1 = self.send_feat_request("/home/erwin/Pictures/raw_faces/Xi1.jpg")
        feat2 = self.send_feat_request("/home/erwin/Pictures/raw_faces/Xi3.jpeg")

        simility = self.send_dist_request(feat1[0][1], feat2[0][1])
        print("simility: %s" % simility)

        self.sock.close()

if __name__ == '__main__':
    face_client = FaceClient()
    face_client.start("")