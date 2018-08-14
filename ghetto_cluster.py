#! python3.x

"""
Usage: 

config.txt:
---------------
master config: test-master:/Users/austind/src/ghetto-cluster/test-config.txt
ignore suffix: .DS_Store, .localized

master: test-master:/Users/austind/src/ghetto-cluster/master
slave: test-slave:/Users/austind/src/ghetto-cluster/slave
ignore suffix: index.html
---------------
Lines are tokenized: "token with spaces: arguments",
The first section is "global", and must contain the term 
"master config:"; slaves will first copy this file down 
from the master and re-load it.  Changes to this file 
will propagate when the slave runs.

After the "global" section, the first "master:" token designates
a master directory, and should be followed by one or more "slave:" 
lines.  Multiple slaves are a-OK.  "ignore suffix:" and "options:" 
tokens are fine in any order

"options:" is not currently defined.


$0 -c <config file> [ -h hostname ] [ -t ]

for this host,
  "master" folders are scanned & a list of checksums is maintained
    "master" folders are not yet scanned for bitrot
  "slave" folders are synced (pull only) from master, the contents
    of master-folder matches the contents of slave-folder, like
    rsync master-folder/ slave-folder
  "slave" folders are not yet (but could future be) scanned for bitrot
"""

import sys, getopt, time, os, signal, subprocess, platform, logging, daemonize
import config, file_state, elapsed, persistent_dict
from utils import logger_str, str_to_duration, duration_to_str
from threading import Thread


class GhettoClusterNode:
    def __init__(self, context):
        self.context = context
        self.config = config.Config.instance()


    # returns a list
    def build_ignorals(self):
        ignorals = [ ".ghetto_cluster" ]  # sync'd separately
        global_ignore_suffix = self.config.getConfig("global", \
                                                    "ignore suffix")
        if type(global_ignore_suffix) is str:
            ignorals.append(global_ignore_suffix)
        elif global_ignore_suffix is not None:
            ignorals += global_ignore_suffix
        local_ignore_suffix = self.config.getConfig(self.context, \
                                                    "ignore suffix")
        if type(local_ignore_suffix) is str:
            ignorals.append(local_ignore_suffix)
        elif local_ignore_suffix is not None:
            ignorals += local_ignore_suffix
        return ignorals


    def ignoring(self, ignorals, dirent):
        for suffix in ignorals:
            # we only ignore suffixes "magically"
            if dirent.endswith(suffix):
                return True
        return False


    def scan(self, gen_checksums = False):
        self.logger.debug(f"scanning {self.path}")
        ignorals = self.build_ignorals()
        cwd = os.getcwd()
        os.chdir(self.path)
        changed = self.scandir(".", ignorals)
        os.chdir(cwd)
        if self.removeDeleteds():
            changed = True
        return changed


    # recursively scan a directory; populate self.states
    # FQDE = fully qualified directory entry: a full path for
    # the file (relative to the master/slave base dir)
    def scandir(self, path, ignorals, gen_checksums = False):
        changed = False
        if path.startswith("./"):
            path = path[2:]
        self.logger.debug(f"scanning path {path}, ignoring {ignorals}")
        try:
            direntries = os.listdir(path)
        except (FileNotFoundError, PermissionError):
            return None
        for dirent in sorted(direntries):
            if self.ignoring(ignorals, dirent): 
                continue
            # not DEAD yet!
            if dirent.find(".ghetto_cluster") != -1:
                self.logger.warning("This should not happen: lost files!")
                self.logger.warning(f"{path}/{dirent} should have been ignored")
                continue
            fqde = f"{path}/{dirent}"
            self.logger.debug(f"scanning {fqde}")
            if os.path.isdir(fqde):
                if self.scandir(fqde, ignorals, gen_checksums):
                    changed = True
                continue
            if not self.states.contains_p(fqde):
                # no FQDE: (maybe) checksum & write it
                self.logger.debug(f"new: {fqde}")
                actualState = file_state.FileState(fqde, gen_checksums)
                self.states.set(fqde, actualState.to_dict())
                changed = True
            else:
                # FQDE: no checksum...
                actualState = file_state.FileState(fqde, False)
                if actualState.maybechanged(self.states.get(fqde)):
                    # ... maybe changed.  (maybe) checksum + write
                    self.logger.debug(f"changed: {fqde}")
                    actualState = file_state.FileState(fqde, gen_checksums)
                    self.states.set(fqde, actualState.to_dict())
                    changed = True
                else:
                    # ... probably same.  preserve the old one (touch it)
                    self.states.touch(fqde)
        return changed


    def removeDeleteds(self):
        changed = False
        for fqde in self.states.clean_keys():
            self.logger.debug(f"removed: {fqde}")
            self.states.delete(fqde)
            changed = True
        self.states.write()
        return changed



# hosts a top-level path, as master (or future, slave)
class GhettoClusterMaster(GhettoClusterNode):
    def __init__(self, context):
        super().__init__(context)
        self.master = self.config.get_master_for_context(context)
        self.path = config.path_for(self.master)
        persistent_dict_file = f"{self.path}/.ghetto_cluster/" \
                               f"master.{context}.json"
        self.states = persistent_dict.PersistentDict(persistent_dict_file)
        self.logger = logging.getLogger(logger_str(__class__))



class GhettoClusterSlave(GhettoClusterNode):
    def __init__(self, context, dest, source):
        super().__init__(context)
        self.dest = dest
        if not source.endswith("/"):
            self.source = source + "/"
        else:
            self.source = source
        hostname = config.host_for(dest)
        self.path = config.path_for(dest)
        self.states_filename = f"{self.path}/.ghetto_cluster/" \
                               f"{hostname}.{context}.json"
        self.states = persistent_dict.PersistentDict(self.states_filename)
        self.testing = False
        self.verbose = \
            self.config.getConfig("global", "verbose", False)
        self.logger = logging.getLogger(logger_str(__class__))


    def scan(self, gen_checksums = False): 
        self.logger.info("Scanning...")
        changed = super().scan(gen_checksums)
        if changed:
            self.logger.info("Pushing states to master")
            file_state.rsync(self.states_filename, 
                            f"{self.source}/.ghetto_cluster", 
                            stfu=True)
        return changed


    def pull(self):
        self.logger.debug(f"Pulling {self.dest} from {self.source}")
        # self.sync_filelist()
        # cwd = os.getcwd()
        # os.chdir(self.path)
        # self.pullfiles(".")
        args = []
        options = self.config.getConfig(self.context, "options")
        if options is not None and "delete" in options:
            self.logger.debug("DELETING")
            args = [ "--delete" ]
                # , "--delete-excluded" ] # this will screw with slaves
        elif self.verbose:
            self.logger.info("Probably not deleting")
        ignorals = self.build_ignorals()
        if len(ignorals) > 0:
            args += [ f"--exclude={item}" for item in ignorals ]

        if self.testing:
            # test mode: strip out hostnames for the rsync
            source = config.path_for(self.source)
        else:
            source = self.source

        dest = config.path_for(self.dest)
        self.logger.info("Starting rsync ...")
        file_state.rsync(source, dest, args)
        # os.chdir(cwd)

        


class GhettoCluster:
    def __init__(self, configfile, hostname = None):
        self.hostname = hostname
        self.config = config.Config.instance()
        self.config.init(configfile, hostname)
        self.testing = self.config.getConfig("global", "testing")
        self.logger = logging.getLogger(logger_str(__class__))


    def start(self):
        while True:
            self.logger.info("Starting")
            self.config.load()
            masters = self.config.get_masters_for_host(self.hostname)
            if len(masters.items()) > 0:
                for context, master in masters.items():
                    self.logger.info(f"{context}: {master}")
                    gcm = GhettoClusterMaster(context)
                    gcm.scan()
                    self.get_status(context, master, False)
                self.logger.info("masters are complete.")
            else:
                self.logger.info("master of None")
            slaves = self.config.get_slaves_for_host(self.hostname)
            if len(slaves.items()) > 0:
                for context, slave in slaves.items():
                    self.logger.info(f"{context}: {slave}")
                    source = self.config.get_master_for_context(context)
                    dest = config.path_for(slave)
                    gcs = GhettoClusterSlave(context, slave, source)
                    # gcs.pull()
                    # gcs.scan()
                    puller = Thread(target=gcs.pull)
                    self.logger.info("Starting pull thread")
                    puller.start()
                    timer = elapsed.ElapsedTimer()
                    while puller.is_alive():
                        if timer.once_every(15):
                            scanner = Thread(target=gcs.scan)
                            self.logger.debug("Starting scan thread")
                            scanner.start()
                            scanner.join()
                            self.logger.debug("Scan thread finished")
                        else:
                            time.sleep(1) # spin, but not hard
                    gcs.scan()
                self.logger.info("Slaves are complete")
            else:
                self.logger.info("slave to noone")
            CYCLE = str_to_duration(self.config.getConfig("global", \
                                                            "CYCLE", "24h"))
            self.logger.info(f"Done (sleeping for {duration_to_str(CYCLE)})")
            time.sleep(CYCLE)


    def stats_forever(self):
        while True:
            self.config.load()
            masters = self.config.get_masters_for_host(self.hostname)
            if len(masters.items()) > 0:
                for context, master in masters.items():
                    self.get_status(context, master)
                print("-=" * 20 + "-")
            else:
                print("Not master of anything")
            time.sleep(30)


    def get_status(self, context, master, to_console = True):
        prefix = f"{config.path_for(master)}/.ghetto_cluster/"
        master_file = f"{prefix}/master.{context}.json"
        master_states = persistent_dict.PersistentDict(master_file)
        (files, bytes) = self.sizeof(master_states)
        print(f"Master: {context}: {config.path_for(master)} " + \
                f"{files} files, {bytes/2**30:.2f}GB")
        master = self.config.get_master_for_context(context)
        for slave in self.config.get_slaves_for_context(context):
            msg = self.inspect_slave(master, master_states, context, slave)
            if to_console:
                print(msg)
            else:
                self.logger.info(msg)
        if to_console:
            print()

    
    def inspect_slave(self, master, master_states, context, slave):
        prefix = f"{config.path_for(master)}/.ghetto_cluster/"
        hostname = config.host_for(slave)
        slave_file = f"{prefix}/{hostname}.{context}.json"
        slave_states = persistent_dict.PersistentDict(slave_file)
        if self.config.getConfig("global", "verbose"):
            msg = f"{slave} ::\n"
            missing = mismatch = extra = 0
            lines = 0
            for fqde, master_state in master_states.items():
                if slave_states.contains_p(fqde):
                    slave_states.touch(fqde)
                    slave_state = slave_states.get(fqde)
                    if master_state["size"] != slave_state["size"]:
                        mismatch += 1
                        if lines < 10:
                            msg += f"\tmismatch: {fqde} "
                            if slave_state["ctime"] > master_state["ctime"]:
                                msg += "slave is newer"
                            else:
                                # TODO: tell how stale it is
                                msg += f"{duration_to_str(master_state['ctime'] - slave_state['ctime'])} stale"
                            msg += "\n"
                            lines += 1
                else:
                    missing += 1
                    if lines < 10:
                        msg += f"\tmissing: {fqde}\n"
                        lines += 1
                if lines == 10:
                    msg += "\t...\n"
                    lines = 11
            extra = len(slave_states.clean_keys())
            if missing + mismatch + extra != 0:
                pct = 100 * len(slave_states.items()) / len(master_states.items())
                if pct > 100:
                    pct = 100
                if int(pct) == 100:
                    pct = 99
                msg += f"\tmissing: {missing} ({pct:.0f}% complete); " + \
                        f"mismatched: {mismatch}; " + \
                        f"extra: {extra}" 
            else:
                msg = "Complete: " + msg[:-4]
        else:
            (target_files, target_bytes) = \
                self.sizeof(master_states)
            (nlines, nbytes) = \
                self.sizeof(slave_states)
            pct_complete = int(100*nlines/target_files)
            if nlines == target_files:
                msg = f"Complete: {slave}"
            elif nlines < target_files:
                msg = f"{pct_complete:3d}% {nlines}/{target_files}: {slave}"
            else:
                msg = f"WARNING: too many files in slave " + \
                        f"{config.host_for(slave)}\n" + \
                        f"\t{nlines}/{target_files}: {slave}"
        return msg


    def sizeof(self, states):
        nfiles = nbytes = 0
        for fqde, state in states.items():
            nfiles += 1
            nbytes += state["size"]
        return (nfiles, nbytes)


    # it's grep -c, basically
    def grep_c(self, filename, string):
        n = 0
        with open(filename, "r") as file:
            for line in file:
                if string in line:
                    n += 1
        return n



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


def setup_logging():
    logger = logging.getLogger(__file__)
    logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                        level=logging.INFO)
    return logger


def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "a:c:h:dknstv")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(1)
    cfg = config.Config.instance()
    action = "repl"
    configfile = "config.txt"
    as_daemon = False
    hostname = platform.node()
    status = False
    testing = False
    verbose = False
    logger = logging.getLogger(__name__)
    for opt, arg in opts:
        if opt == "-n":
            cfg.setConfig("global", "dryrun", True)
            logger.info("DRYRUN mode: no files will be copied")
        elif opt == "-a":
            action = arg
        elif opt == "-c":
            configfile = arg
        elif opt == "-d":
            as_daemon = True
        elif opt == "-h":
            hostname = arg
        elif opt == "-k":
            kill_and_exit()
        elif opt == "-s":
            status = True
        elif opt == "-t":
            cfg.setConfig("global", "testing", True)
        elif opt == "-v":
            cfg.setConfig("global", "verbose", True)
            verbose = True
        else:
            assert False, "Unhandled option"
    gc = GhettoCluster(configfile, hostname)
    if status:
        gc.stats_forever()
    if verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO
    if as_daemon:
        global PIDFILE
        LOGFILE = os.path.expanduser(cfg.getConfig(
                        "global", "LOGFILE", "gc.log"))
        fh = logging.handlers.RotatingFileHandler( \
                LOGFILE, maxBytes=10*2**20, backupCount=5)
        fh.setLevel(logLevel)
        logger.addHandler(fh)
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            handlers=[fh], level=logging.DEBUG)
        logger.info("Starting daemon...")
        keep_fds = [fh.stream.fileno()]
        daemon = daemonize.Daemonize(app="ghetto-cluster", \
                         pid=PIDFILE, action=gc.start, \
                         keep_fds=keep_fds)
        daemon.start()
    else:
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logLevel)
        logger.setLevel(logLevel)
        gc.start()



if __name__ == "__main__":
    main(sys.argv)
    # gcm = GhettoClusterMaster("/Volumes/Media_ZFS/Movies/action")
    # gcm.scan()
