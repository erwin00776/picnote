import threading

class ThreadCore(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def echo(self, msg):
        print("hello, %s" % msg)

    def run(self):
        self.echo("mars")

if __name__ == '__main__':
    t = ThreadCore()
    t.echo("hi")

