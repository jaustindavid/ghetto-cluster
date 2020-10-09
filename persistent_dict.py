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
    def __init__(self, filename, lazy_timer=0, **kwargs):
        self.masterFilename = filename
        self.transactionName = None
        self.data = {}
        self.logger = logging.getLogger(logger_str(__class__))
        self.lazy_timer = lazy_timer
        self.dirty = False
        self.read()
        self.clear_dirtybits()
        self.timer = elapsed.ElapsedTimer()
        if "metadata" in kwargs:
            self.metadata_key = kwargs["metadata"]
        else:
            self.metadata_key = "__metadata__"


    # destructor
    def __del__(self):
        return # causes problems with file IO in some cases, racey
        if self.dirty:
            self.logger.debug("one last write")
            self.write()


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
                self.data = json.load(statefile, strict=False)
                if self.data is None:
                    self.logger.debug("json.load() -> self.data is None")
                    self.data = {}
        except json.decoder.JSONDecodeError as err:
            self.logger.warn(err)
            os.rename(filename, f"{filename}.busted")
            self.logger.warn(f"whoopsie, JSONDecodeError;" \
                        f" saved in {filename}.busted")
            self.data = {}
        self.logger.debug(f"read {len(self.data.items())} items")
        self.dirty = False


    def write(self, verbose = False):
        if self.transactionName is not None:
            filename = f"{self.masterFilename}.{self.transactionName}"
        else:
            filename = self.masterFilename
        self.logger.debug(f"writing data: {filename}")
        self.mkdir(filename)
        tmpfile = f"{filename}.tmp"
        ntries = 0
        # TODO: protect this ... better
        while os.path.exists(tmpfile) and ntries < 6:
            logger.debug(f"{tmpfile} exists; waiting 5")
            time.sleep(5)
            ntries += 1
        with open(tmpfile, "w") as statefile:
            json.dump(self.data, statefile, sort_keys=True, indent=4)
        try:
            os.rename(tmpfile, filename)
        except:
            self.logger.warn(f"failed to rename tmpfile; ignoring")
        self.dirty = False


    def lazy_write(self):
        if self.lazy_timer == 0 or self.timer.elapsed() > self.lazy_timer:
            self.write()
            self.timer.reset()


    def mkdir(self, filename):
        dir = os.path.dirname(filename)
        if dir != "" and not os.path.exists(dir):
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
        self.dirty = True
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
        if self.metadata_key in self.data:
            dupe = self.data.copy()
            del dupe[self.metadata_key]
            return dupe.items()
        return self.data.items()
            

    def contains_p(self, key):
        return key in self.data


    def clear_dirtybits(self):
        self.dirtybits = {}


    # returns the keys which haven't been touched
    # https://stackoverflow.com/questions/3462143/get-difference-between-two-lists
    def clean_keys(self):
        return [key for key in self.data if key not in self.dirtybits]


if __name__ == "__main__":
    import hashlib
    pd = PersistentDict("pd.json", lazy_write=60)
    for key in range(100000):
        h = hashlib.sha256()
        h.update(f"this is a {key}".encode())
        pd.set(key, h.hexdigest())
        if key % 1000 == 0:
            print(key)
