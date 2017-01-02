import os
import time
import face_client

class CheckFaces:
    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.processed = {}
        self.is_running = True
        self.face_client = face_client.FaceClient()
        self.human_dir = os.path.join(self.dirpath, "humans")
        self.humans = {}

    def load_known_faces(self):
        for root, dirs, files in os.walk(self.human_dir):
            for filename in files:
                if not self.is_images(filename):
                    continue
                filepath = os.path.join(root, filename)
                faces_feature = self.face_client.send_feat_request(filepath)
                human_name = os.path.basename(root)
                human_features = self.humans.get(human_name, [])
                for face_id, face_feature in faces_feature:
                    human_features.append(face_feature)
                self.humans[human_name] = human_features

    def is_images(self, filename):
        if filename is None or len(filename) < 3:
            return False
        dot_pos = filename.rfind(".")
        suffix = filename[dot_pos + 1:]
        if suffix == 'jpg' or suffix == 'jpeg' or suffix == 'png':
            return True

    def scan_images(self):
        images_list = []
        for root, dirs, files in os.walk(self.dirpath):
            for filename in files:
                if self.is_images(filename):
                    images_list.append(os.path.join(root, filename))
        return images_list

    def shutdown(self):
        self.is_running = False

    def handle_new_image(self, filepath):
        faces_features = self.face_client.send_feat_request(filepath)
        if faces_features == -1:
            print("%s new image process error." % filepath)
            return None, None
        self.processed[filepath] = faces_features

        max_simility = 0.0
        simility_human = None
        for human_name, human_features in self.humans.items():
            similities = 0.0
            for face_id, face_feature in faces_features:
                for human_feature in human_features:
                    simi = self.face_client.send_dist_request(human_feature, face_feature)
                    similities += float(simi)
            similities /= len(human_features)
            if similities > max_simility:
                max_simility = similities
                simility_human = human_name
        print(filepath, simility_human, max_simility)
        return simility_human, max_simility

    def run(self):
        self.load_known_faces()
        while self.is_running:
            images_list = self.scan_images()
            for image_path in images_list:
                if image_path in self.processed:
                    continue
                self.handle_new_image(image_path)
            time.sleep(20)

if __name__ == "__main__":
    check_face = CheckFaces("/home/erwin/raspbian_photos")
    check_face.run()
