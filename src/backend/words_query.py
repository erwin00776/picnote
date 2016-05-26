
import os
import sys
import json
import nltk
sys.path.append("..")
from common.base import *
from store.redis_store import RedisStore


class WordsQuery:
    def __init__(self):
        self.store = RedisStore()

    def query(self, word_list):
        """
        1) add rank
        2) more precise
        3)
        :param word_list:
        :return:
        """
        pics = set([])
        for w in word_list:
            id_list = self.store.query_query2id(w)
            pics = pics.union(id_list)
        return list(pics)