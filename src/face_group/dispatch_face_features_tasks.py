#!/usr/bin/env python

import os
import math
import subprocess
import threading
import multiprocessing

import redis


class FaceFeatureGenerator:
    """
    Redis Key:
        face.done[s]: set of images path that analysis done.
        face.corrupted[s]: set of image path that corrupted.
        face.features{}: hash map of features of images, {image_path, face_id, features}
    """
    def __init__(self):
        self.redis_cli = redis.Redis()
        self.base_dir = "/home/erwin/face_group"

    @staticmethod
    def scan_images_dir(dirname, todo_filename):
        todo_file = None
        if todo_filename is not None:
            todo_file = open(todo_filename, 'w')
        image_list = []
        for root, dirs, files in os.walk(dirname):
            for filename in files:
                if 'jpg' in filename or 'jpeg' in filename:
                    filepath = os.path.join(root, filename)
                    image_list.append(filepath)
                    if todo_file is not None:
                        todo_file.write(filepath + "\n")
        if todo_file is not None:
            todo_file.close()
        return image_list

    @staticmethod
    def is_features_process_running(process_name="generate_features_from_list"):
        cmd = "ps xuf | grep -v grep | grep %s -c" % process_name
        try:
            ret_val = subprocess.check_output(cmd, shell=True)
            ret_val = int(ret_val.strip())
        except subprocess.CalledProcessError:
            ret_val = 0
        return ret_val > 0

    def restart_features_process(self, todo_filepath, done_filepath, features_filepath):
        cmd = "export LD_LIBRARY_PATH=$HOME/face_group/build:$LD_LIBRARY_PATH; cd %s; " \
              "./generate_features_from_list %s %s %s" \
              % (self.base_dir, todo_filepath, done_filepath, features_filepath)
        try:
            print(cmd)
            ret_val = subprocess.call(cmd, shell=True)
        except:
            ret_val = -1
        return ret_val

    def handle_done_file(self, done_filepath):
        if not os.path.exists(done_filepath):
            return -2
        try:
            done_file = open(done_filepath, 'r')
            done_images = done_file.readlines()
            if done_images is None or len(done_images) == 0:
                return 0
            if len(done_images) == 1:
                self.redis_cli.sadd("face.corrupted", done_images[0])
                return 0

            if done_images[-2] != done_images[-1]:
                self.redis_cli.sadd("face.corrupted", done_images[-1])
                done_set = set(done_images[:-1])
            else:
                done_set = set(done_images)
            for done in done_set:
                self.redis_cli.sadd("face.done", done)

            done_file.close()
        except IOError as e:
            print(e.message)
            return -1
        return 0

    def handle_features_files(self, features_filepath):
        if not os.path.exists(features_filepath):
            return -2
        try:
            feats_file = open(features_filepath, 'r')
            feats = feats_file.readlines()
            for feat in feats:
                face, face_id, face_feat = feat.split(":")
                self.redis_cli.hmset('face.features', {face: (face_id, face_feat)})
            feats_file.close()
        except IOError as e:
            print(e.message)
            return -1
        return 0

    def generate_todo_list(self, image_list, todo_filepath):
        todo_out = open(todo_filepath, 'w')
        todo_list = []
        for image in image_list:
            if self.redis_cli.sismember("face.done", image):
                continue
            todo_out.write(image + "\n")
            todo_list.append(image)
        todo_out.close()
        return todo_list

    def worker_run(self, worker_name, todo_list):
        todo_filepath = os.path.join(self.base_dir, "todo_list_%s" % worker_name)
        done_filepath = os.path.join(self.base_dir, "done_list_%s" % worker_name)
        features_filepath = os.path.join(self.base_dir, "features_list_%s" % worker_name)
        current_todo_list = self.generate_todo_list(todo_list, todo_filepath)
        ret_val = -1
        while ret_val != 0:
            print("worker-%s todo_list length: %d" % (worker_name, len(current_todo_list)))
            ret_val = self.restart_features_process(todo_filepath, done_filepath, features_filepath)
            self.handle_features_files(features_filepath)
            self.handle_done_file(done_filepath)
            if ret_val != 0:
                # re-generate todo_file by original todo_list & done_file
                print("WARN prepare to restart face feature worker.")
                current_todo_list = self.generate_todo_list(todo_list, todo_filepath)
                return

    @staticmethod
    def chunks(arr, m):
        n = int(math.ceil(len(arr) / float(m)))
        return [arr[i:i + n] for i in range(0, len(arr), n)]

    def master_run(self, dir_name,  worker_num=1, batch_size=100):
        image_list = self.scan_images_dir(dir_name, None)
        filter_list = []
        for image in image_list:
            if self.redis_cli.sismember("face.done", image):
                continue
            if self.redis_cli.sismember("face.corrupted", image):
                continue
            filter_list.append(image)

        l = 0
        while l < len(filter_list):
            r = l + batch_size if (l + batch_size) <= len(filter_list) else len(filter_list) + 1
            batch_list = filter_list[l: r]
            todo_lists = self.chunks(batch_list, worker_num)
            thread_list = []
            worker_id = 0
            for todo_list in todo_lists:
                thread = threading.Thread(name="face_feature_worker_%d" % worker_id,
                                          target=self.worker_run,
                                          args=(worker_id, todo_list,))
                thread_list.append(thread)
                thread.start()
                worker_id += 1
            for thread in thread_list:
                thread.join()
            l = r + 1

if __name__ == '__main__':
    image_dir_path = "/home/erwin/Pictures/raw_faces/"
    image_dir_path = "/home/erwin/git-repos/facescrub/download"
    face_feature_generator = FaceFeatureGenerator()
    # face_feature_generator.scan_images_dir(image_dir_path, "/home/erwin/face_group/todo.list")
    face_feature_generator.master_run(image_dir_path)
