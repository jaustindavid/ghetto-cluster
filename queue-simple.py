#!/usr/bin/python2.7

import sys, getopt, time, os

class Queue:
    def __init__(self, filename):
        self.filename = filename

    def read(self):


def read(filename):
    if not os.path.exists(filename):
       with open(filename, "w+"): pass
    with open(filename, "r") as file:
       contents = file.readlines()
    return contents

def write(filename, contents):
    with open(filename, "w+") as file:
        file.writelines(contents)


def show(filename):
    contents = read(filename)
    for line in contents:
        print line.strip()

def enqueue(filename, string):
    value = [int(time.time()), string]
    element = str(int(time.time())) + ": " + string + "\n"
    print "Enqueueing", value, "to", filename
    contents = read(filename)
    contents.append(element)
    write(filename, contents)

def dequeue(filename):
    print "Dequeueing from", filename
    contents = read(filename)
    if contents:
        value = str(contents[0]).strip().split(": ", 1)
        value[0] = int(value[0])
        print "Removed %d:: %s" % (value[0], value[1])
        print value
        write(filename, contents[1:])
        return value
    else:
        return None

def test():
    import os
    filename = "test-queue.txt"
    if not os.path.exists(filename):
        with open(filename, "w+"): pass
    show(filename)
    enqueue(filename, "first line")
    show(filename)
    enqueue(filename, "second line")
    show(filename)
    dequeue(filename)
    show(filename)
    os.remove(filename)

if __name__ == "__main__":
	print "Running tests"
	test()
