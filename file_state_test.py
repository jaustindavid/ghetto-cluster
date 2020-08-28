#!/usr/local/bin/python3.6

import unittest, file_state, os, subprocess, shutil, pprint

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        global tempdir
        # print(f"Building {tempdir}")
        subprocess.call(["mkdir", "-p", "tmp"])
        with open("/dev/zero") as zero:
            one_k = zero.read(1024);
        filename = f"tmp/file.1k"
        outfile = open(filename, "w+")
        outfile.write(one_k)
        global pp
        pp = pprint.PrettyPrinter(indent=4)

    def tearDown(self):
        global tempdir
        shutil.rmtree(tempdir)

    def test_1_empty_scandir(self):
        state = file_state.FileState("tmp/file.1k")

tempdir = "tmp"

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
