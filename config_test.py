#!/usr/bin/env python3

import unittest, config, pprint, logging

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        global pp
        pp = pprint.PrettyPrinter(indent=4)
        global cfg 
        cfg = config.Config.instance()
        cfg.init("config-test.txt", testing=True)

    def tearDown(self):
        pass

    def test_load(self):
        pp.pprint(cfg.config)
        self.assertEqual(len(cfg.config.keys()), 5)

    def test_options(self):
        ignores = cfg.getConfig(0, "ignore suffix")
        self.assertEqual(".DS_Store" in ignores, True)
        self.assertEqual(cfg.getOption("logfile"), "~/gc/gc.log")
        self.assertEqual(cfg.getOption("randomoption", "False"), "False")
        cfg.setOption("randomoption", "True")
        pp.pprint(cfg.config)
        self.assertEqual(cfg.getOption("randomoption", "True"), "True")

    def test_ignorals(self):
        pp.pprint(cfg.get_ignorals(1))
        self.assertTrue(".DS_Store" in cfg.get_ignorals(1))
        pp.pprint(cfg.get_ignorals(2))
        self.assertTrue(".DS_Store" in cfg.get_ignorals(2))
        self.assertTrue("streams" in cfg.get_ignorals(2))

    def test_rest(self):
        sources = cfg.get_sources_for_host("srchost")
        self.assertEqual(sources[2], "srchost:/path/to/files")
        replicas = cfg.get_replicas_for_host("dsthost1")
        self.assertEqual(replicas[3], "dsthost1:/mnt/morefiles")
        self.assertTrue("bwlimit=10m" in cfg.getConfig(2, "rsync options"))

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', 
            level=logging.DEBUG)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
