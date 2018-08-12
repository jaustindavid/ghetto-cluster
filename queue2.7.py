#!/usr/bin/python2.7

import sys, getopt, time, os

class Element:
    def __init__(self, value):
        if ": " in value:
            stuff = value.strip().split(": ", 1)
            if (stuff[0].isdigit()):
                self.timestamp = int(stuff[0])
                self.value = stuff[1]
            else:
                self.timestamp = int(time.time())
                self.value = value
        else:
            self.timestamp = int(time.time())
            self.value = value

    def __str__(self):
        return str(self.timestamp) + ": " + self.value


class AtomicQueue:
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        if not os.path.exists(self.filename):
            with open(self.filename, "w+"): pass
        with open(self.filename, "r") as file:
            contents = list()
            for line in file:
               contents.append(Element(line.strip()))
        return contents

    def write(self, contents):
        with open(self.filename, "w+") as file:
            for element in contents:
                file.write(str(element) + "\n")

    def show(self, verbose=True):
        contents = self.read()
        for element in contents:
            if verbose:
                print element

    def enqueue(self, string, verbose=False):
        element = Element(string)
        if verbose:
            print "Enqueueing", element
        contents = self.read()
        contents.append(element)
        self.write(contents)

    def dequeue(self, verbose=False):
        if verbose:
            print "Dequeueing"
        contents = self.read()
        if contents:
            element = contents[0]
            if verbose:
                print "Removed %d>%s" % (element.timestamp, element.value)
            self.write(contents[1:])
            return element
        else:
            return None
    
    def reset(self, verbose=False):
        os.remove(self.filename)


def selftest():
    import os
    filename = "test-queue.txt"
    q = AtomicQueue(filename)
    if not os.path.exists(filename):
        with open(filename, "w+"): pass
    q.show()
    q.enqueue("first line", True)
    q.show()
    q.enqueue("second line", True)
    q.show()
    q.dequeue(True)
    q.show()
    q.dequeue(True)
    q.show()
    os.remove(filename)

if __name__ == "__main__":
	print "Running tests"
	selftest()
