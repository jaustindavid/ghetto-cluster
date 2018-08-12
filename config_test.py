#!/usr/local/bin/python3.6

import unittest, config

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_config(self):
        cfg = config.Config.instance()
        cfg.init("config.txt")
        print(cfg.config)
        self.assertEquals(cfg.getConfig("global", "ignore suffix"), \
                            ".DS_STORE")
        self.assertEquals(cfg.getConfig('mini:/Volumes/Media_ZFS/Movies', \
                            "ignore suffix"), "index.html")
        self.assertEquals(cfg.getConfig("global", "BLOCKSIZE"), '10240')

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
