#!/usr/local/bin/python3.6

import unittest, elapsed, time

class TestTimerMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_timer(self):
        start = int(time.time())
        timer = elapsed.ElapsedTimer()
        time.sleep(3)
        self.assertEquals(int(timer.elapsed()), 3)
        timer.reset()
        self.assertEquals(int(timer.elapsed()), 0)
        time.sleep(3)
        self.assertEquals(int(timer.elapsed()), 3)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTimerMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
