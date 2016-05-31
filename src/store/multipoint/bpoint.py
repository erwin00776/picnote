
from seqfile import LogFile

class BPoint:
    """ Daemon """
    def __init__(self, name):
        self.name = name
        self.basepath = ''
        self.logfile = LogFile()

    def run(self):
        pass

if __name__ == "__main__":
    bpoint = BPoint()
    bpoint.run()