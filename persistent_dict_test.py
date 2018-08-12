#!/usr/local/bin/python3.6

import unittest, persistent_dict, os

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        os.remove("testfile.txt")


    def test_set(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("thing", 1)
        pd.set("thing two", "two")
        self.assertEquals(pd.get("thing"), 1)
        self.assertEquals(pd.get("thing two"), "two")

    def test_io(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("thing", 1)
        pd.set("thing two", "two")
        pd2 = persistent_dict.PersistentDict("testfile.txt")
        self.assertEquals(pd2.get("thing"), 1)
        self.assertEquals(pd2.get("thing two"), "two")


    def test_dirty(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("one", 1)
        pd.set("two", 1)
        pd.set("three", 1)
        pd.clear_dirtybits()
        pd.set("three", 3)
        pd.set("two", 2)
        self.assertEquals(pd.clean_keys()[0], "one")


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
