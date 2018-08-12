#! python 3.x

"""
from persistentdict import PersistentDict

pd = PersistentDict("dict.json")
pd.startTransaction()
pd.set()
pd.set()
pd.closeTransaction()
"""

import os, json, logging
from utils import logger_str
import elapsed, config

class PersistentDict:
    def __init__(self, filename):
        self.masterFilename = filename
        self.transactionName = None
        self.data = {}
        self.logger = logging.getLogger(logger_str(__class__))
        self.read()
        self.clear_dirtybits()
        self.timer = elapsed.ElapsedTimer()


    def read(self, verbose = False):
        if self.transactionName is not None:
            filename = f"{self.masterFilename}.{self.transactionName}"
        else:
            filename = self.masterFilename
        self.logger.debug(f"reading from {filename}")
        if not os.path.exists(filename):
            self.logger.debug("whoopsie, no file")
            return None
        try:
            with open(filename, "r") as statefile:
                self.data = json.load(statefile)
                if self.data is None:
                    self.logger.debug("json.load() -> self.data is None")
                    self.data = {}
        except json.decoder.JSONDecodeError:
            os.rename(filename, f"{filename}.busted")
            self.logger.warn(f"whoopsie, JSONDecodeError;" \
                        f" saved in {filename}.busted")
            self.data = {}
        self.logger.debug(f"read {len(self.data.items())} items")


    def write(self, verbose = False):
        if self.transactionName is not None:
            filename = f"{self.masterFilename}.{self.transactionName}"
        else:
            filename = self.masterFilename
        self.logger.debug(f"writing data: {filename}")
        self.mkdir(filename)
        with open(f"{filename}.tmp", "w") as statefile:
            json.dump(self.data, statefile, sort_keys=True, indent=4)
        os.rename(f"{filename}.tmp", filename)


    def lazy_write(self):
        cfg = config.Config.instance()
        LAZY_TIMER = cfg.getConfig("global", "LAZY_WRITE", 5)
        if self.timer.elapsed() > LAZY_TIMER:
            self.write()
            self.timer.reset()


    def mkdir(self, filename):
        dir = os.path.dirname(filename)
        if not os.path.exists(dir):
            os.makedirs(dir)


    def startTransaction(self, transactionName = "tmp"):
        self.transactionName = transactionName
        self.write()


    def closeTransaction(self, verbose = False):
        filename = f"{self.masterFilename}.{self.transactionName}"
        os.rename(filename, self.masterFilename)


    def touch(self, key):
        self.dirtybits[key] = 1


    def set(self, key, value):
        self.data[key] = value
        self.touch(key)
        self.lazy_write()


    def get(self, key):
        return self.data[key]


    def delete(self, key):
        try:
            del self.data[key]
            del self.dirtybits[key]
        except KeyError:
            pass


    def items(self):
        return self.data.items()
            

    def contains_p(self, key):
        return key in self.data


    def clear_dirtybits(self):
        self.dirtybits = {}


    # returns the keys which haven't been touched
    # https://stackoverflow.com/questions/3462143/get-difference-between-two-lists
    def clean_keys(self):
        return [key for key in self.data if key not in self.dirtybits]
