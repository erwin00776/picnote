
import os
import json
import pickle

class SeqFile:
    def __init__(self, path, sid, replications=3):
        if isinstance(sid, int):
            self.sid = str(sid)
        else:
            self.sid = sid
        self.replications = replications       # TODO
        self.checkpoint_len = 16
        self.path = path
        self.tmp_filename = os.path.join(self.path, 'picnote.sequences-%s.tmp' % self.sid)
        self.seqs_filename = os.path.join(self.path, 'picnote.sequences-%s' % self.sid)
        self.tmp_seqs_file = None
        self.tmp_seqs = None
        self.seqs = None
        self.seqs_file = None
        self.init_done = self.try_recover()

    def try_recover(self):
        ret = True
        if not self.load():
            ret = False
            raise Exception("recover.load error.")
        if not self.checkpoint():
            ret = False
            raise Exception("recover.checkpoint error.")
        return ret

    def load(self):
        if not os.path.exists(self.tmp_filename):
            self.tmp_seqs = []
        if not os.path.exists(self.seqs_filename):
            self.seqs = []

        try:
            if self.tmp_seqs is None:
                self.tmp_seqs = []
                self.tmp_seqs_file = open(self.tmp_filename, 'r')
                for line in self.tmp_seqs_file.readlines():
                    self.tmp_seqs.append(json.loads(line))
                self.tmp_seqs_file.close()

        except IOError as e:
            print(e.message)
            return False

        try:
            if self.seqs is None:
                self.seqs_file = open(self.seqs_filename, 'r')
                self.seqs = pickle.load(self.seqs_file)
                self.seqs_file.close()
        except EOFError as e:
            if self.seqs_file is not None:
                self.seqs_file.close()
            self.seqs = []

        assert (self.seqs is not None)
        try:
            self.tmp_seqs_file = open(self.tmp_filename, 'w')
            self.seqs_file = open(self.seqs_filename, 'w')
            return True
        except IOError as e:
            print(e.message)
            return False

    def checkpoint(self):
        ''' truncate tmp seqs to seqs '''
        try:
            if self.tmp_seqs is None or len(self.tmp_seqs) == 0:
                return True
            for item in self.tmp_seqs:
                self.seqs.append(item)
            self.tmp_seqs_file.seek(0)
            self.tmp_seqs_file.truncate()
            self.seqs_file.seek(0)
            self.seqs_file.truncate()
            pickle.dump(self.seqs, self.seqs_file)
            self.tmp_seqs = []
            # remove the tmp seqs file.
            print("checkpoint done: %d %d" % (len(self.tmp_seqs), len(self.seqs)))
        except IOError as e:
            print("checkpoint " + e.message)
            return False
        return True

    def record(self, item, force=False):
        """
        :param item: a hash.
        :return: True: done, False: negative
        """
        ret = True
        try:
            s = json.dumps(item)
            self.tmp_seqs.append(item)
            self.tmp_seqs_file.write(s + "\n")
            self.tmp_seqs_file.flush()
            if (len(self.tmp_seqs) > self.checkpoint_len) or force:
                self.checkpoint()
        except IOError as e:
            ret = False
        return ret

    def merge(self):
        ''' merge old items in seqs '''
        new_seqs = []
        for item in self.seqs:
            if item['replications'] < self.replications:
                new_seqs.append(item)
        self.seqs = None
        self.seqs = new_seqs
        self.checkpoint()

    def close(self):
        self.merge()
        self.tmp_seqs_file.close()
        self.seqs_file.close()

if __name__ == "__main__":
    sf = SeqFile("/home/erwin/tmp", 1)
    if not sf.init_done:
        print("init error.")
    for i in range(50):
        r = i / 10 + 1
        item = {'type': 'add', 'filepath': '/home/erwin/...', 'replications': r}
        sf.record(item)
    sf.close()
