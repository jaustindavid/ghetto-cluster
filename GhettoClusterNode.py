#!/usr/bin/env python3

import config, logging, pprint, time, signal, os, sys
from GhettoClusterSource import GhettoClusterSource
from GhettoClusterReplica import GhettoClusterReplica
from utils import str_to_duration, duration_to_str


class WakeupException(Exception):
    pass

class TermException(Exception):
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


    def restore(self):
        sources = self.config.get_sources_for_host(self.hostname)
        if len(sources.items()) > 0:
            print(f"To restore stuff on {self.hostname}:")
            for context, source, in sources.items():
                gcs = GhettoClusterSource(context, source, self.testing)
                print("")
                gcs.restore()
        else:
            print("I host no sources; nothing to restore")


    def run(self, scan_only=False):
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
                if scan_only:
                    gcr.scan_only()
                else:
                    gcr.run()
                gcr.get_status(brief=True)
        else:
            self.logger.info(f"I host no replicas")


    def run_forever(self):
        try:
            signal.signal(signal.SIGHUP, self.wakeup)
            signal.signal(signal.SIGTERM, self.go_peacefully)
            while True:
                self.run()
                CYCLE = str_to_duration(self.config.getOption("cycle", "24h"))
                self.logger.info(f"Sleeping for {duration_to_str(CYCLE)}" + \
                                    f" in PID {os.getpid()}")
                self.logger.debug("send SIGHUP to wake up")
                time.sleep(CYCLE)
        except WakeupException:
            self.logger.warn(f"Restarting as requested (SIGHUP)")
            signal.signal(signal.SIGHUP, signal.SIG_DFL)
        except TermException:
            self.logger.warn(f"Exiting for SIGTERM")
            sys.exit()
        except KeyboardInterrupt:
            self.logger.warn(f" Exiting...")
            sys.exit()


    # TODO: this
    def cleanup_gc_dir(self):
        self.logger.warn("Cleanup time")
        sources = self.config.get_sources_for_host(self.hostname)
        valid_files = []
        if len(sources.items()) > 0:
            for context, source, in sources.items():
                # append source
                valid_files.append(f"")
                # append all replicas
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


    def wakeup(self, signum, frame):
        raise WakeupException from None

    def go_peacefully(self, signum, frame):
        raise TermException from None


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
