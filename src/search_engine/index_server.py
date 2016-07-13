# coding=utf-8
import os
import sys
import jieba
from index_store import RedisIndexStore
from src.backend.utils.superior_thread import SuperiorThread
from src.backend.utils.dfs_log import LOG


"""
field:
    1: content
    2: title
    4: meta
    8:
"""


class IndexServer(SuperiorThread):
    def __init__(self, index_store):
        self.index_store = index_store
        # jieba.analyse.set_stop_words("stop_words.txt")

    def index_dir(self, dirname):
        for root, dirnames, filenames in os.walk(dirname):
            for fn in filenames:
                file_path = os.path.join(root, fn)
                file_id = self.index_store.get_file_id(file_path)
                self.index_str(file_path, field=2, file_id=file_id)
                self.index_file(file_path)

    def index_file(self, filename):
        pass

    def index_str(self, text, field, file_id):
        tokenizer = jieba.cut_for_search(text)
        for token in tokenizer:
            token = token.strip()
            if len(token) < 1:
                continue
            LOG.info("%s %d" % (token, file_id))
            self.index_store.add_token(token, field, file_id)

    def run(self):
        pass


if __name__ == "__main__":
    index_server = IndexServer(RedisIndexStore())
    index_server.index_str("hello, world china.", 2, 0)
