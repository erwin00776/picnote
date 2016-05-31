
import os
import sys
import json
import nltk
import hashlib
sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore
from neural_generator import NeuralGenerator

class BaseProcessor:
    def process(self, wordlist):
        raise NotImplementedError()


class FilterProcessor(BaseProcessor):
    def __init__(self):
        self.stopwords = None
        self.load_stopwords()

    def load_stopwords(self):
        p = os.path.join(STOPWORD_PATH, 'stopwords_en.txt')
        self.stopwords = set([])
        if os.path.exists(p):
            with open(p, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    self.stopwords.add(line)
        print("loading stopwords " + str(self.stopwords))
        LOGGER.info("loading stopwords %s" % str(self.stopwords))

    def process(self, wordlist):
        rlist = []
        for word in wordlist:
            if word in self.stopwords:
                continue
            rlist = rlist + [word]
        return rlist


class Stemmer(BaseProcessor):
    """ It does not promise stem the word to the original one. """
    def __init__(self, default_lang='english'):
        # self.stemmer = nltk.SnowballStemmer(default_lang)
        self.stemmer = nltk.PorterStemmer()

    def process(self, wordlist):
        rlist = []
        for word in wordlist:
            w = self.stemmer.stem(word)
            if w is not None and len(w) > 0:
                rlist.append(w)
        return rlist


class ExtendProcessor(BaseProcessor):
    def __init__(self):
        pass

    def process(self, wordlist):
        # extend by word2vec
        return wordlist


class WordsProcessors:
    def __init__(self):
        self.processors = []
        self.register(FilterProcessor())
        self.register(Stemmer())
        self.register(ExtendProcessor())

    def register(self, processors):
        self.processors.append(processors)

    def process(self, words):
        querys = []
        for word in words:
            rlist = [word]
            for p in self.processors:
                rlist = p.process(rlist)
                if rlist is None or len(rlist) == 0:
                    break
            querys = querys + rlist
        return querys


class PicturesNote:
    def __init__(self):
        self.wp = WordsProcessors()
        self.store = RedisStore()

    def gen_notes(self, path, nums=None):
        if nums is None:
            nums = 0
            files = os.listdir(path)
            for filename in files:
                if os.path.isdir(filename) or filename[-3].lower() in PICTURE_SUFFIXS:
                    continue
                nums = nums + 1
        ng = NeuralGenerator()
        items = ng.generator(path, nums)
        for item in items:
            filepath = item['file_name']
            caption = str(item['caption'])
            words = caption.split(' ')
            querys = self.wp.process(words)
            print(querys)
            filename = os.path.basename(filepath)
            filename = filename.encode('utf-8')
            md5 = hashlib.md5()
            md5.update(filename)
            id = md5.hexdigest()
            self.add2store(querys, id, caption)
        return querys

    def add2store(self, querys, id, caption):
        self.store.replace_meta(id, 'desc', caption)
        for q in querys:
            self.store.add_query2id(q, id)


if __name__ == '__main__':
    pn = PicturesNote()
    pn.gen_notes("/home/erwin/pictures")
