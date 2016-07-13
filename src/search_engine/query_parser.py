# coding = utf-8

import os
import sys
import jieba


class QueryParser:
    def __init__(self, index_store):
        self.index_store = index_store

    def parse(self, query_string):
        r = {}
        seg_list = jieba.cut_for_search(query_string)
        for token in seg_list:
            fmap = self.index_store.query(token)
            for k, v in fmap.items():
                if k in r:
                    r[k] = r[k].intersection(set(v))
                else:
                    r[k] = set(v)
        return r