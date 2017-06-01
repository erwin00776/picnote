
import os
import sys
import json
import nltk
sys.path.append("..")
#from common.base import *
from src.common.base import *
from src.store.redis_store import RedisStore
#from backend.words_processors import WordsProcessors
from src.backend.handlers.words_processors import WordsProcessors

class WordsQuery:
    def __init__(self):
        self.store = RedisStore()
        self.processor = WordsProcessors()

    def query(self, word_list):
        """
        1) add rank
        2) more precise
        3)
        :param word_list:
        :return:
        """
        pics = None
        wlist = self.processor.process(word_list)
        for w in wlist:
            id_list = self.store.query_query2id(w)
            print(id_list)
            if pics is None:
                pics = id_list
            else:
                pics = pics.intersection(id_list)
        return list(pics)