#!/usr/local/bin/python3.6

import unittest, queue, os, time

class TestAtomicQueueMethods(unittest.TestCase):

    def setUp(self):
        global q, filename, line
        with open(filename, "w+") as file:
            file.writelines(line)

    def tearDown(self):
        global filename
        os.remove(filename)
        
    def test_element(self):
        line1 = "0123.4: an invalid string"
        line2 = "543: a valid one"
        element1 = queue.Element(line1)
        self.assertEqual(element1.value, line1)
        element2 = queue.Element(line2)
        self.assertEqual(element2.timestamp, int(line2[0:3]))

    def test_read(self):
        global filename, line
        contents = q.read()
        self.assertEqual(str(contents[0]), line)

    def test_write(self):
        global filename, line
        contents = [queue.Element(line)]
        q.write(contents)
        self.assertTrue(os.path.exists(filename))
        with open(filename, "r") as file:
            newContents = file.readlines()
        self.assertEqual(newContents[0].strip(), line)

    def test_queue(self):
        global filename, line
        q.show(False)
        element = q.dequeue()
        self.assertEqual(element.timestamp, 123)
        self.assertEqual(element.value, "Test Input")
        element = q.dequeue()
        self.assertEqual(element, None)
        q.enqueue("moar inputz")
        q.enqueue("e'en moar inputz")
        element = q.dequeue()
        self.assertEqual(element.timestamp, int(time.time()))
        self.assertEqual(element.value, "moar inputz")
        element = q.dequeue()
        self.assertEqual(element.timestamp, int(time.time()))
        self.assertEqual(element.value, "e'en moar inputz")
        

filename = "test_queue.txt"
line = "123: Test Input"
q = queue.AtomicQueue(filename)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAtomicQueueMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
