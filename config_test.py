#!/usr/local/bin/python3.6

import unittest, config

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_config(self):
        cfg = config.Config.instance()
        cfg.init("sample-config-complete.txt")
        print(cfg.config)
        ignores = cfg.getConfig("global", "ignore suffix")
        self.assertEquals(".DS_Store" in ignores, True)
        ignores = cfg.getConfig('mini:/Volumes/Media_ZFS/Movies', \
                                "ignore suffix")
        self.assertEquals("index.html" in ignores, True)
        self.assertEquals(cfg.getConfig("global", "LOGFILE"), "~/gc/gc.log")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
