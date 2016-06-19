import os
import sys


class BaseProcessor:
    def __init__(self, store_path, filename):
        self.store_path = store_path
        self.filename = filename
        self.allow_suffix = set([])

    def match(self):
        bname = os.path.basename(self.filename)
        dot = bname.rfind('.')
        if dot > -1 and bname[dot+1:].lower() in self.allow_suffix:
            return True
        return False

    def process(self):
        raise "not implement yet."


class DefaultProcessor(BaseProcessor):
    def __init__(self, store_path, filename):
        BaseProcessor.__init__(self, store_path, filename)

    def match(self):
        """ allow process all type of files. """
        return True

    def process(self):
        bname = os.path.basename(self.filename)
        return False, os.path.join(self.store_path, bname)


class PhotoProcessor(BaseProcessor):
    def __init__(self, store_path, filename):
        BaseProcessor.__init__(self, store_path, filename)
        self.allow_suffix = set(['jpg'])

    def process(self):
        pass


class DocProcessor(BaseProcessor):
    def __init__(self, store_path, filename):
        BaseProcessor.__init__(self, store_path, filename)
        self.allow_suffix = set(['doc', 'pdf'])

    def process(self):
        pass


class MusicProcessor(BaseProcessor):
    def __init__(self, store_path, filename):
        BaseProcessor.__init__(self, store_path, filename)
        self.allow_suffix = set(['mp3', 'wma'])

    def process(self):
        pass


