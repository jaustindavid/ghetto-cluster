#!/usr/bin/env python3

import os, logging
import config, persistent_dict, file_state

class Scanner:
    def __init__(self, path, ignorals, state_filename):
        self.logger = logging.getLogger("gc.scanner")
        self.config = config.Config.instance()
        self.path = path
        self.ignorals = ignorals
        self.states = persistent_dict.PersistentDict(state_filename, \
                                self.config.getOption("LAZY_WRITE", 5))


    def ignoring(self, ignorals, dirent):
        for suffix in ignorals:
            # we only ignore suffixes "magically"
            if dirent.endswith(suffix):
                return True
        return False


    def removeDeleteds(self):
        for fqde in self.states.clean_keys():
            self.logger.debug(f"removed: {fqde}")
            self.states.delete(fqde)
        self.states.write()


    # recursively scan a directory; populate self.states
    # FQDE = fully qualified directory entry: a full path for
    # the file (relative to the source/replica base dir)
    def scandir(self, path, ignorals):
        if path.startswith("./"):
            path = path[2:]
        # ignore .gc directory entirely
        if path == ".gc":
            # print("skipping .gc")
            return
        self.logger.debug(f"scanning path {path}, ignoring {ignorals}")
        try:
            direntries = os.listdir(path)
        except (FileNotFoundError, PermissionError):
            return
        for dirent in sorted(direntries):
            if self.ignoring(ignorals, dirent):
                continue
            fqde = f"{path}/{dirent}"
            self.logger.debug(f"scanning {fqde}")
            if os.path.isdir(fqde):
                self.scandir(fqde, ignorals)
            else:
                state = file_state.FileState(fqde, False)
                self.states.set(fqde, state.to_dict())


    def scan(self):
        if not os.path.exists(self.path):
            self.logger.warn(f"cannot scan: {self.path} does not exist")
            return True
        else:
            self.logger.info(f"  Scanning {self.path}")
        cwd = os.getcwd()
        os.chdir(self.path)
        changed = self.scandir(".", self.ignorals)
        os.chdir(cwd)
        self.removeDeleteds()
        self.states.write()
