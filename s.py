#! python 3.x

from singleton import Singleton

@Singleton
class S:
    def __init__(self):
        print("made an s")

    def setup(self, data):
        self.data = data

    def act(self):
        print(self.data)
