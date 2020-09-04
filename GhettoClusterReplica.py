#!/usr/bin/env python3

import config, logging, pprint
import persistent_dict, rsync, scanner, statusfier
import signal, elapsed, time, os
from threading import Thread
from utils import str_to_duration, duration_to_str


# a Node may have 0 or more Replicas; 
# they may run() in an independent thread
class GhettoClusterReplica:
    def __init__(self, context, replica, testing=False):
        self.logger = logging.getLogger("gc.GhettoClusterReplica")
        self.config = config.Config.instance()
        self.context = context
        self.replica = replica
        self.testing = testing
        self.verbose = self.config.getOption("verbose", "False") == "True"
        self.source = self.config.get_source_for_context(self.context)
        if not self.source.endswith("/"):
            self.source = self.source + "/"
        hostname = config.host_for(replica)
        self.path = config.path_for(replica)
        self.states_filename = f"{self.path}/.gc/{hostname}.{context}.json"


    def pull(self):
        self.logger.debug("Pulling")
        # print(f"running for {self.path} <= {self.source}")
        args = [ "-a" ]
        if self.verbose:
            args.append("-v")
        options = self.config.getConfig(self.context, "rsync options")
        if len(options) > 0:
            args += [ f"--{option}" for option in options ]
        ignorals = self.config.get_ignorals(self.context)
        if len(ignorals) > 0:
            args += [ f"--exclude={item}" for item in ignorals ]
        self.logger.debug("Starting rsync pull...")
        self.logger.debug(f"rsync {' '.join(args)} {self.source} {self.path}")
        rsync.rsync(self.source, self.path, args)


    def push(self):
        self.logger.info("  Pushing states to source")
        if self.verbose:
            args = [ "-v" ]
        else:
            args = []
        rsync.rsync(self.states_filename, f"{self.source}.gc", 
                    args, stfu=False)


    def get_status(self):
        self.logger.debug(f"Getting status for {self.context}:{self.replica}")
        stats = statusfier.Statusfier()
        stats.get_status_for_replica(self.context, self.replica)


    def run(self):
        self.logger.info(f"Running, {self.context}:{self.path}")
        puller = Thread(target=self.pull)
        self.logger.debug("Starting pull thread")
        puller.start()
        timer = elapsed.ElapsedTimer()
        ignorals = self.config.get_ignorals(self.context)
        scn = scanner.Scanner(self.path, ignorals, self.states_filename)
        while puller.is_alive():
            if timer.once_every(60):
                scn.scan()
                self.push()
            else:
                time.sleep(1)
        puller.join()
        scn.scan()
        self.push()
        self.logger.info(f"Finished: {self.context}:{self.path}")
        

    def scan_only(self):
        self.logger.info(f"Scanning {self.context}:{self.path}")
        ignorals = self.config.get_ignorals(self.context)
        scn = scanner.Scanner(self.path, ignorals, self.states_filename)
        scn.scan()
        self.push()
        self.logger.info(f"Finished: {self.context}:{self.path}")


import unittest
class TestMyMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testReplica(self):
        cfg = config.Config.instance()
        replicae = GhettoClusterReplica(1, cfg.config[1]["replicas"][0], testing=True)
        replicae.run()


if __name__ == "__main__":
    logger = logging.getLogger('gc')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    cfg = config.Config.instance()
    cfg.init("config-test.txt", testing=True)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMyMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
