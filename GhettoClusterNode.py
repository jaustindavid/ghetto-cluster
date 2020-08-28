#!/usr/bin/env python3

import config, logging, pprint, time, signal, os, sys
from GhettoClusterSource import GhettoClusterSource
from GhettoClusterReplica import GhettoClusterReplica
from utils import str_to_duration, duration_to_str


class WakeupException(Exception):
    pass


# a Node has 0 or more sources, 0 or more replicas
class GhettoClusterNode:
    def __init__(self, configfile, hostname = None, testing=False):
        self.logger = logging.getLogger("gc.GhettoClusterNode")
        self.hostname = hostname
        self.config = config.Config.instance()
        self.config.init(configfile, hostname, testing=testing)
        self.testing = testing
        self.verbose = self.config.getOption("verbose", "False") == "True"


    def stats(self, pretty=False):
        self.config.load()
        sources = self.config.get_sources_for_host(self.hostname)
        if len(sources.items()) > 0:
            for context, source in sources.items():
                gcs = GhettoClusterSource(context, source)
                gcs.get_status()
                if pretty:
                    self.logger.info("")
        else:
            self.logger.info("No sources for me")
            if pretty:
                self.logger.info("")

        replicas = self.config.get_replicas_for_host(self.hostname)
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(replicas)
        if len(replicas.items()) > 0:
            for context, replica in replicas.items():
                # self.get_replica_status(context, replica)
                gcr = GhettoClusterReplica(context, replica)
                gcr.get_status()
                if pretty:
                    self.logger.info("")
        else:
            self.logger.info("No replicas for me")


    def stats_forever(self):
        while True:
            self.stats(pretty=True)
            self.logger.info("-=" * 20 + "-")
            time.sleep(30)


    def run(self):
        self.logger.info(f"Running for {self.hostname}")
        self.config.load()
        sources = self.config.get_sources_for_host(self.hostname)
        if len(sources.items()) > 0:
            for context, source, in sources.items():
                gcs = GhettoClusterSource(context, source, self.testing)
                gcs.run()
                gcs.get_status()
        else:
            self.logger.info(f"I host no sources")
        replicas = self.config.get_replicas_for_host(self.hostname)
        if len(replicas.items()) > 0:
            for context, replica in replicas.items():
                gcr = GhettoClusterReplica(context, replica, self.testing)
                gcr.run()
                gcr.get_status()
        else:
            self.logger.info(f"I host no replicas")


    def run_forever(self):
        while True:
            self.run()
            try:
                signal.signal(signal.SIGHUP, self.wakeup)
                CYCLE = str_to_duration(self.config.getOption("cycle", "24h"))
                self.logger.info(f"Sleeping for {duration_to_str(CYCLE)}" + \
                                    f" in PID {os.getpid()}")
                self.logger.debug("send SIGHUP to wake up")
                time.sleep(CYCLE)
            except WakeupException:
                self.logger.warn(f"Restarting as requested (SIGHUP)")
                signal.signal(signal.SIGHUP, signal.SIG_DFL)
            except KeyboardInterrupt:
                self.logger.warn(f" Exiting...")
                sys.exit()


    def wakeup(self, signum, frame):
        raise WakeupException from None


import unittest
class TestMyMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSource(self):
        node = GhettoClusterNode("config-test.txt", "srchost", testing=True)
        node.stats()
        node.run()

    def testReplica(self):
        node = GhettoClusterNode("config-test.txt", "dsthost1", testing=True)
        node.stats()
        node.run()


if __name__ == "__main__":
    logger = logging.getLogger('gc')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMyMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
