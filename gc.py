#!/usr/bin/env python3

#
# 0 0 * * * ~/gc/gc.py -c ~/gc/config.txt -d # -h HOSTNAME
#


import sys, getopt, os, signal, logging, platform, daemonize
import config, GhettoClusterNode, GhettoClusterReplica
import pid


PIDFILE = "/var/run/lock/gc.pid"


def lock_or_die():
    try:
        pidlock = pid.PidFile(PIDFILE)
        pidlock.create()
        # print(f"Locked on {PIDFILE}")
        return pidlock
    except pid.PidFileError:
        print(f"Failed to lock; check {PIDFILE}")
        sys.exit(1)


def release(pidlock):
    pidlock.close()


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
        opts, args = getopt.getopt(sys.argv[1:], "1c:dh:knrstvz")
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
    scan_only = False
    restore = False
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
        elif opt == "-r":
            restore = True
        elif opt == "-s":
            status = True
        elif opt == "-t": # TODO: remove
            cfg.setOption("testing", "True")
        elif opt == "-v":
            cfg.setOption("verbose", "True")
            verbose = True
        elif opt == "-z":
            scan_only = True
        else:
            assert False, "Unhandled option"
    if verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO
    gcn = GhettoClusterNode.GhettoClusterNode(configfile, hostname)

    if as_daemon:
        logfile = os.path.expanduser(cfg.getOption("logfile", "gc.log"))
        fh = logging.handlers.RotatingFileHandler( \
                logfile, maxBytes=10*2**20, backupCount=5)
        fh.setLevel(logLevel)
        logger.addHandler(fh)
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            handlers=[fh], level=logging.DEBUG)
        once = True
        status = False
        scan_only = False
        # fall through to gc.run()
    else:
        logging.basicConfig(format='%(message)s', level=logLevel)

    if restore:
        gcn.restore()
    elif status:
        if once:
            gcn.stats()
        else:
            gcn.stats_forever()
    elif scan_only:
        gcn.run(True)
    else:
        pidlock = lock_or_die()
        if once:
            gcn.run()
        else:
            gcn.run_forever()
        release(pidlock)


if __name__ == "__main__":
    main(sys.argv)
