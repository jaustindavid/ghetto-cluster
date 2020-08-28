#!/usr/bin/env python3

# <class '__main__.GhettoCluster'> -> GhettoCluster
def logger_str(classname):
    string = str(classname)
    return string[string.index('.')+1:string.index("'>")]


# 64 -> "1m4s"
def duration_to_str(seconds):
    string = ""
    if seconds > 24*60*60:
        days = int(seconds / (24*60*60))
        seconds = seconds % (24*60*60)
        string += f"{days}d"
    if seconds > 60*60:
        hours = int(seconds / (60*60))
        seconds = seconds % (60*60)
        string += f"{hours}h"
    if seconds > 60:
        minutes = int(seconds / 60)
        seconds = seconds % 60
        string += f"{minutes}m"
    if seconds > 0:
        string += f"{int(seconds)}s"
    return string


# "6h1s" -> 21601
# method is really simplistic: ##d -> ## * (24 hrs); ##s -> ##
#   ... iteratively walk the string, scaling ## by d/h/m/s
# side effect: other characters are ignored, 1z6 -> 16 (seconds)
def str_to_duration(string):
    nr = 0
    total = 0
    while string != "":
        if string[0].isdigit():
            nr = nr*10 + int(string[0])
        elif string[0] == "d":
            total += 24*3600 * nr
            nr = 0
        elif string[0] == "h":
            total += 3600 * nr
            nr = 0
        elif string[0] == "m":
            total += 60 * nr
            nr = 0
        elif string[0] == "s":
            total += nr
            nr = 0
        string = string[1:]
    total += nr
    return total


import unittest
class TestMyMethods(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_str_to_duration(self):
        self.assertEqual(str_to_duration("60s"), 60)
        self.assertEqual(str_to_duration("60m"), 3600)
        self.assertEqual(str_to_duration("2h"), 2*3600)
        self.assertEqual(str_to_duration("1h60s"), 3600 + 60)
        self.assertEqual(str_to_duration("61m"), 3600 + 60)
        self.assertEqual(str_to_duration("1h1m1s"), 3600 + 60 + 1)

    def test_duration_to_str(self):
        self.assertEqual(duration_to_str(60), "60s")
        self.assertEqual(duration_to_str(3600), "60m")
        self.assertEqual(duration_to_str(2*3600), "2h")
        self.assertEqual(duration_to_str(3600 + 60), "1h60s")
        self.assertEqual(duration_to_str(3600 + 60 + 1), "1h1m1s")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMyMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
