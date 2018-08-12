#!/usr/local/bin/python3.6

import queue, getopt, sys

def main():
    try:
    	opts, args = getopt.getopt(sys.argv[1:], "a:q:")
    except getopt.GetoptError as err:
    	print(err)
    	sys.exit(1)
    # print "Opts:", opts
    # print "Args:", args
    filename = "queue.txt"
    action = "show"
    for opt, arg in opts:
    	if opt == "-q":
    		filename = arg
    	elif opt == "-a":
    		action = arg
    	else:	
    		assert False, "Unhandled option"
    q = queue.AtomicQueue(filename)
    if action == "show":
    	q.show()
    elif action.startswith("enq"):
    	q.enqueue(" ".join(args))
    elif action.startswith("deq"):
    	q.dequeue()
    elif action.startswith("res"):
    	q.reset()
    else:
    	assert False, "Undefined action"
        

if __name__ == "__main__":
    # print "Queue starting"
    main()
