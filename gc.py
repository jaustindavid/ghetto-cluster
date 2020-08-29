#!/usr/bin/env python3

#
# 0 0 * * * ~/gc/gc.py -c ~/gc/config.txt -d # -h HOSTNAME
#

import sys, getopt, os, signal, logging, platform, daemonize
import config, GhettoClusterNode, GhettoClusterReplica

PIDFILE = "/tmp/gc.pid"

def kill_and_exit():
    global PIDFILE
    if os.path.exists(PIDFILE):
        with open(PIDFILE, "r") as pidfile:
            contents = pidfile.readlines()
        pid = int(contents[0])
        print(f"Killing {pid}")
        os.kill(pid, signal.SIGTERM)
        sys.exit()
    else:
        print(f"{PIDFILE} does not exist; nothing to kill")
        sys.exit(2)
    sys.exit(3)


def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "1c:dh:knstv")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(1)
    cfg = config.Config.instance()
    configfile = "config.txt"
    as_daemon = False
    hostname = platform.node()
    once = False
    status = False
    testing = False
    verbose = False
    logger = logging.getLogger(__name__)
    for opt, arg in opts:
        if opt == "-1":
            once = True
        elif opt == "-c":
            configfile = arg
        elif opt == "-d":
            as_daemon = True
        elif opt == "-h":
            hostname = arg
        elif opt == "-k":
            kill_and_exit()
        elif opt == "-n":
            cfg.setOption("dryrun", "True")
            logger.info("DRYRUN mode: no files will be copied")
        elif opt == "-s":
            status = True
        elif opt == "-t":
            cfg.setOption("testing", "True")
        elif opt == "-v":
            cfg.setOption("verbose", "True")
            verbose = True
        else:
            assert False, "Unhandled option"
    gcn = GhettoClusterNode.GhettoClusterNode(configfile, hostname)
    if verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO
    if as_daemon:
        global PIDFILE
        logfile = os.path.expanduser(cfg.getOption("logfile", "gc.log"))
        fh = logging.handlers.RotatingFileHandler( \
                logfile, maxBytes=10*2**20, backupCount=5)
        fh.setLevel(logLevel)
        logger.addHandler(fh)
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            handlers=[fh], level=logging.DEBUG)
        keep_fds = [fh.stream.fileno()]
        logger.info("Starting daemon . . .")
        fh.flush()
        daemon = daemonize.Daemonize(app="ghetto-cluster", \
                         pid=PIDFILE, action=gcn.run_forever, \
                         keep_fds=keep_fds)
                         # logger=logger)
        print("Starting ...")
        daemon.start()
        # never returns
        return
    logging.basicConfig(format='%(message)s', level=logLevel)
    # logger.setLevel(logLevel)
    if status:
        gcn.stats_forever()
        # never returns
        return
    if once:
        gcn.run()
    else:
        gcn.run_forever()


if __name__ == "__main__":
    main(sys.argv)
