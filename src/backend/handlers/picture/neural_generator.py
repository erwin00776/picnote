
import os
import sys
import json
sys.path.append("..")
#from common.base import *
from src.common.base import *


class NeuralGenerator:
    def __init__(self):
        pass

    def generator(self, path, nums):
        vis_path = "%s/vis/vis.json" % NEURAL_TALK
        if os.path.exists(vis_path):
            os.remove(vis_path)
        export = "export PATH=/usr/local/cuda/bin:$PATH;" \
                 " export LD_LIBRARY_PATH=/home/erwin/cbuild/CBLAS/lib:/usr/local/cuda/lib64" \
                 "f$LD_LIBRARY_PATH"
        cmd = "cd %s; /bin/sh %s/eval.sh %s %d" % (NEURAL_TALK, NEURAL_TALK, path, nums)
        print(cmd)
        os.system(cmd)
        return self.get_result()

    def generate_by_list(self, list_file_path, nums):
        vis_path = "%s/vis/vis.json" % NEURAL_TALK
        if os.path.exists(vis_path):
            os.remove(vis_path)
        cmd = "cd %s; /bin/sh %s/eval_list.sh %s %d" % (NEURAL_TALK, NEURAL_TALK, list_file_path, nums)
        print(cmd)
        os.system(cmd)
        return self.get_result()

    def get_result(self):
        vis_path = "%s/vis/vis.json" % NEURAL_TALK
        if not os.path.exists(vis_path):
            return None
        try:
            with open(vis_path, 'r') as f:
                data = f.readline()
                items = json.loads(data)
                for item in items:
                    print("%s %s" % (item['file_name'], item['caption']))
                return items
        except IOError as e:
            return None


if __name__ == '__main__':
    ng = NeuralGenerator()
    ng.generator("/home/erwin/pictures", 100)
    # ng.get_result()