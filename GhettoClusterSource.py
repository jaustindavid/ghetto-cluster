#!/usr/bin/env python3

import config, logging, pprint
import persistent_dict, scanner, statusfier
import os, os.path

# a Node may have 0 or more Sources
# they may run() in an independent thread
class GhettoClusterSource:
    def __init__(self, context, source, testing=False):
        self.logger = logging.getLogger("gc.GhettoClusterSource")
        self.config = config.Config.instance()
        self.context = context
        self.source = source
        self.testing = testing
        self.verbose = self.config.getOption("verbose", "False") == "True"
        self.path = config.path_for(source)
        hostname = config.host_for(source)
        self.states_filename = f"{self.path}/.gc/{hostname}.{context}.json"
        self.states = persistent_dict.PersistentDict(self.states_filename, \
                        self.config.getOption("LAZY_WRITE", 5))


    # TODO: this doesn't work.  It needs to happen at a higher level,
    # there may be multiple source contexts for a single source folder
    def cleanup_gc_dir(self):
        self.logger.warn("Cleanup time")
        hostname = config.host_for(self.source)
        valid_files = [ f"{hostname}.{self.context}.json" ]
        replicas = self.config.get_replicas_for_context(self.context)
        if len(replicas) > 0:
            for replica in replicas:
                statefile = f"{config.host_for(replica)}.{self.context}.json"
                valid_files.append(statefile)
        else:
            print("wat")
        print(f"Valid files: {valid_files}")
        json_files = [f for f in os.listdir(self.path) \
                            if os.path.isfile(os.path.join(self.path, f)) \
                                and f.endswith("json")]
        for json_file in json_files:
            if json_file not in valid_files:
                self.logger.info(f"extraneous file: {json_file}")


    def get_status(self):
        self.logger.debug(f"Getting status for {self.context}:{self.source}")
        stats = statusfier.Statusfier()
        stats.get_status_for_source(self.context, self.source)


    def run(self):
        self.logger.info(f"Running for {self.context}:{self.source}")
        ignorals = self.config.get_ignorals(self.context)
        scn = scanner.Scanner(self.path, ignorals, self.states_filename)
        scn.scan()
        # TODO: clean up any invalid replicas
