
import os
import json
import pickle

class SeqFile:
    def __init__(self, path, sid):
        self.sid = sid
        self.backup_count = 3       # TODO
        self.checkpoint_len = 64
        self.path = path
        self.tmp_filename = os.path.join(self.path, 'picnote.sequences-%d.tmp' % self.sid)
        self.seqs_filename = os.path.join(self.path, 'picnote.sequences-%d' % self.sid)
        self.tmp_seqs_file = None
        self.tmp_seqs = None
        self.seqs = None
        self.seqs_file = None
        self.init_done = self.try_recover()

    def try_recover(self):
        if not self.load():
            raise Exception("recover.load error.")
        if not self.checkpoint():
            raise Exception("recover.checkpoint error.")

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
            if self.seqs is None:
                self.seqs_file = open(self.seqs_filename, 'r')
                self.seqs = pickle.load(self.seqs_file)
                self.seqs_file.close()
            assert ((self.seqs is not None) and (len(self.seqs) >= 0))
        except IOError as e:
            print(e.message)
            return False

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
            pickle.dump(self.seqs, self.seqs_file)
            self.tmp_seqs = []
            # remove the tmp seqs file.
        except IOError as e:
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
            if item[''] < self.backup_count:
                new_seqs.append(item)
        self.seqs = None
        self.seqs = new_seqs


if __name__ == "__main__":
    sf = SeqFile("/home/erwin/tmp", 1)
    if not sf.init_done:
        print("init error.")
    for i in range(50):
        r = i / 10 + 1
        item = {'type': 'add', 'filepath': '/home/erwin/...', 'replications': r}
        sf.record(item)
    sf.merge()
