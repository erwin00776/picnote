# coding = utf-8
import sys
import os
import redis


class IndexStore:
    def __init__(self):
        pass

    def store(self, word, file_id):
        pass

    def query(self, word):
        pass

    def get_file_id(self, filename):
        pass

    def get_id2filename(self, file_id):
        pass

    def add_token(self, token, field, file_id):
        pass


class RedisIndexStore(IndexStore):
    def __init__(self):
        self.redis_cli = redis.Redis()

    def get_file_id(self, filename):
        file_id = self.redis_cli.incr("file_id")
        self.redis_cli.hset("file_id_mapping", file_id, filename)
        return file_id

    def get_id2filename(self, file_id):
        filename = self.redis_cli.hget('file_id_mapping', file_id)
        return filename

    def add_token(self, token, field, file_id):
        self.redis_cli.sadd("%s#%d" % (token, field), file_id)

    def query(self, token):
        r = {}
        r[1] = self.redis_cli.smembers("%s#1" % token)
        r[2] = self.redis_cli.smembers("%s#2" % token)
        r[4] = self.redis_cli.smembers("%s#4" % token)
        r[8] = self.redis_cli.smembers("%s#8" % token)
        return r


class FileIndexStore(IndexStore):
    pass
