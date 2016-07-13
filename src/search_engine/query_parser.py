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
            token = token.strip()
            if not token or len(token) < 1:
                continue

            fmap = self.index_store.query(token)
            for field, val in fmap.items():
                if field in r:
                    r[field] = r[field].intersection(set(val))
                else:
                    r[field] = set(val)
        return r