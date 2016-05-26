
import os
import sys
import json
sys.path.append("..")
from common.base import *
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
    def process(self, wordlist):
        return wordlist

class ExtendProcessor(BaseProcessor):
    def process(self, wordlist):
        #extend by word2vec
        return wordlist


class WordsProcessors:
    def __init__(self):
        self.processors = []
        self.register(FilterProcessor())
        self.register(Stemmer())
        self.register(ExtendProcessor())

    def register(self, processors):
        self.processors.append(processors)

    def process(self, path):
        ng = NeuralGenerator()
        items = ng.generator(path, 5)
        for item in items:
            filepath = item['file_name']
            caption = str(item['caption'])
            words = caption.split(' ')
            querys = []
            for word in words:
                rlist = [word]
                for p in self.processors:
                    rlist = p.process(rlist)
                    if rlist is None or len(rlist) == 0:
                        break
                querys = querys + rlist
            print(querys)
        return querys


if __name__ == '__main__':
    wp = WordsProcessors()
    wp.process("/home/erwin/pictures")
