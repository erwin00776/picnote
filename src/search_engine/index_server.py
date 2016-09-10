# coding=utf-8
import os
import sys
import eyed3
import jieba
from index_store import RedisIndexStore
from src.backend.utils.superior_thread import SuperiorThread
from src.backend.utils.dfs_log import LOG
from src.backend.utils.uncompress import *
import xml.etree.ElementTree
import xml.etree.ElementTree as ET


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
        self.text_suffixs = set(['txt', 'md'])

    def index_dir(self, dirname):
        for root, dirnames, filenames in os.walk(dirname):
            for fn in filenames:
                file_path = os.path.join(root, fn)
                file_id = self.index_store.get_file_id(file_path)
                self.index_str(file_path, field=2, file_id=file_id)
                self.index_file(file_path)

    def suffix_name(self, filename):
        dot = filename.rfind('.')
        if dot > 0:
            return filename[dot+1:]
        return None

    def index_text_file(self, filename):
        try:
            fp = open(filename, 'r')
            lines = fp.readlines()
            return lines
        except IOError as e:
            LOG.error("can not read file %s (%s)" % (filename, e.message))
            return []

    def index_odt_file(self, filename):
        lines = []
        try:
            tmp_dir = unzip(filename)
        except:
            return lines
        content = os.path.join(tmp_dir, 'content.xml')
        try:
            tree = ET.parse(content)
            root = tree.getroot()
            item_list = root.getchildren()

            for line in item_list[3].itertext():
                lines.append(line)
            return lines
        except xml.etree.ElementTree.ParseError as e:
            print(e.message)
        finally:
            if os.path.exists(tmp_dir):
                os.removedirs(tmp_dir)
        return lines

    def index_music_file(self, filename):
        tag = eyed3.load(filename)
        artist = None
        album = None
        lines = []
        try:
            artist = tag.tag.artist
            album = tag.tag.album
            artist = artist.encode('utf-8')
            album = album.encode('utf-8')
            if not artist:
                lines.append(artist)
            if not album:
                lines.append(album)
        except AttributeError as e:
            pass
        return lines

    def index_photo_file(self, filename):
        pass

    def index_file(self, filename):
        suffix = self.suffix_name(filename)
        if not suffix:
            return
        if suffix in self.text_suffixs:
            self.index_text_file(filename)
        elif suffix == 'odt':
            self.index_odt_file(filename)
        else:
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
