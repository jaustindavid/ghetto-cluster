#!/usr/bin/env python3

import config, logging
from utils import logger_str

class GhettoCluster:
    def __init__(self, configfile, hostname = None):
        self.logger = logging.getLogger("gc.GhettoCluster")
        self.hostname = hostname
        self.config = config.Config.instance()
        self.config.init(configfile, hostname)
        self.testing = self.config.getOption("testing", "True") == "True"


    def get_status(self, context, source):
        self.logger.debug(f"pulling stats for {context}:{source}")
        pass

    def stats(self):
        print("here's what I did")
        self.config.load()
        sources = self.config.get_sources_for_host(self.hostname)
        if len(sources.items()) > 0:
            for context, source in sources.items():
                self.get_status(context, source)
        else:
            print("No sources for me")

    def stats_forever(self):
        while True:
            self.stats()
            print("-=" * 20 + "-")
            time.sleep(15)


    def run(self):
        self.logger.info("Running")
        print("I'm doing the thing")
        self.config.load()
        sources = self.config.get_sources_for_host(self.hostname)
        self.logger.debug(f"sources: {sources}")

    def run_forever(self):
        while True:
            self.run()
            time.sleep(15)


import unittest
class TestMyMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSource(self):
        gc = GhettoCluster("config-test.txt", "srchost")
        gc.stats()
        gc.run()


if __name__ == "__main__":
    logger = logging.getLogger('gc')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMyMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
