#! python3.x

import time

class ElapsedTimer:
    def __init__(self):
        self.reset()

    def elapsed(self):
        return time.time() - self.start

    def reset(self):
        self.start = time.time()
