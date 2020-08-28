#!/usr/bin/env python3

"""
Usage: 

config.txt:
---------------
source config: test-source:/Users/austind/src/ghetto-cluster/test-config.txt
ignore suffix: .DS_Store, .localized

source: test-source:/Users/austind/src/ghetto-cluster/source
replica: test-replica:/Users/austind/src/ghetto-cluster/replica
ignore suffix: index.html
---------------
Lines are tokenized: "token with spaces: arguments",
The first section is "global", and must contain the term 
"source config:"; replicas will first copy this file down 
from the source and re-load it.  Changes to this file 
will propagate when the replica runs.

After the "global" section, the first "source:" token designates
a source directory, and should be followed by one or more "replica:" 
lines.  Multiple replicas are a-OK.  "ignore suffix:" and "options:" 
tokens are fine in any order

"options:" in source context are passed to rsync, with "--" prepended
  e.g. options: delete, bw-limit=10m => rsync --delete --bw-limit=10m


$0 -c <config file> [ -h hostname ] [ -t ]

for this host,
  "source" folders are scanned & a list of checksums is maintained
    "source" folders are not yet scanned for bitrot
  "replica" folders are synced (pull only) from source, the contents
    of source-folder matches the contents of replica-folder, like
    rsync source-folder/ replica-folder
  "replica" folders are not yet (but could future be) scanned for bitrot
"""

import sys, getopt, time, os, signal, subprocess, platform, logging, daemonize
import config, file_state, elapsed, persistent_dict
from utils import logger_str, str_to_duration, duration_to_str
from threading import Thread


class WakeupException(Exception):
    pass


class GhettoClusterNode:
    def __init__(self, context):
        self.context = context
        self.config = config.Config.instance()


    # returns a list
    def build_ignorals(self):
        ignorals = [ ".ghetto_cluster" ]  # sync'd separately
        # global_ignore_suffix = self.config.getConfig("global", \
        #                                             "ignore suffix")
        global_ignore_suffix = self.config.getOption("ignore suffix")
        # TODO: superfluous
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
        if not os.path.exists(self.path):
            self.logger.warn(f"cannot scan: {self.path} does not exist")
            return True
        else:
            self.logger.debug(f"scanning {self.path}")
        ignorals = self.build_ignorals()
        cwd = os.getcwd()
        os.chdir(self.path)
        changed = self.scandir(".", ignorals)
        os.chdir(cwd)
        if self.removeDeleteds():
            changed = True
        self.states.write() # should be redundant
        return changed


    # recursively scan a directory; populate self.states
    # FQDE = fully qualified directory entry: a full path for
    # the file (relative to the source/replica base dir)
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



# hosts a top-level path, as source (or future, replica)
class GhettoClusterSource(GhettoClusterNode):
    def __init__(self, context):
        super().__init__(context)
        self.source = self.config.get_source_for_context(context)
        self.path = config.path_for(self.source)
        persistent_dict_file = f"{self.path}/.ghetto_cluster/" \
                               f"source.{context}.json"
        self.states = persistent_dict.PersistentDict(persistent_dict_file, \
                            self.config.getOption("LAZY_WRITE", 5))
        self.logger = logging.getLogger(logger_str(__class__))



class GhettoClusterReplica(GhettoClusterNode):
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
        self.states = persistent_dict.PersistentDict(self.states_filename, \
                        self.config.getOption("LAZY_WRITE", 5))
        self.testing = False
        self.verbose = self.config.getOption("verbose", "False") == "True"
        self.logger = logging.getLogger(logger_str(__class__))


    def scan(self, gen_checksums = False): 
        self.logger.info("Scanning...")
        changed = super().scan(gen_checksums)
        if changed:
            self.logger.info("Pushing states to source")
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
        # TODO: parse out multiple options
        options = self.config.getConfig(self.context, "rsync options")
        # if options is not None and "delete" in options:
        #     self.logger.debug("DELETING")
        #     args = [ "--delete" ]
        #         # , "--delete-excluded" ] # this will screw with replicas
        args = []
        for option in options:
                args += [ f"--{option}" ]
        # elif self.verbose:
        #     self.logger.info("Probably not deleting")
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
        self.testing = self.config.getOption("testing", "True") == "True"
        self.logger = logging.getLogger(logger_str(__class__))


    def start(self):
        while True:
            self.logger.info("Starting")
            self.config.load()
            sources = self.config.get_sources_for_host(self.hostname)
            print(f"sources: {sources}")
            if len(sources.items()) > 0:
                for context, source in sources.items():
                    self.logger.info(f"{context}: {source}")
                    gcm = GhettoClusterSource(context)
                    gcm.scan()
                    self.get_status(context, source, False)
                self.logger.info("sources are complete.")
            else:
                self.logger.info("source of None")
            replicas = self.config.get_replicas_for_host(self.hostname)
            if len(replicas.items()) > 0:
                for context, replica in replicas.items():
                    self.logger.info(f"{context}: {replica}")
                    source = self.config.get_source_for_context(context)
                    dest = config.path_for(replica)
                    gcs = GhettoClusterReplica(context, replica, source)
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
                self.logger.info("Replicas are complete")
            else:
                self.logger.info("replica to noone")
            try:
                signal.signal(signal.SIGHUP, self.wakeup)
                CYCLE = str_to_duration(self.config.getOption("CYCLE", "24h"))
                self.logger.info(f"Sleeping for {duration_to_str(CYCLE)}" + \
                                    f" in PID {os.getpid()}")
                self.logger.debug("send SIGHUP to wake up")
                time.sleep(CYCLE)
            except WakeupException:
                self.logger.warn(f"Restarting as requested (SIGHUP)")
                signal.signal(signal.SIGHUP, signal.SIG_DFL)


    def wakeup(self, signum, frame):
        raise WakeupException from None


    def stats_forever(self):
        while True:
            self.config.load()
            sources = self.config.get_sources_for_host(self.hostname)
            if len(sources.items()) > 0:
                for context, source in sources.items():
                    self.get_status(context, source)
                print("-=" * 20 + "-")
            else:
                print("Not source of anything")
            time.sleep(30)


    def get_status(self, context, source, to_console = True):
        prefix = f"{config.path_for(source)}/.ghetto_cluster/"
        source_file = f"{prefix}/source.{context}.json"
        source_states = persistent_dict.PersistentDict(source_file)
        (files, bytes) = self.sizeof(source_states)
        # print(f"Source: {context}: {config.path_for(source)} " + \
        print(f"Source: {source}: " + \
                f"{files} files, {bytes/2**30:.2f}GB")   #TODO: pretty this
        source = self.config.get_source_for_context(context)
        for replica in self.config.get_replicas_for_context(context):
            msg = self.inspect_replica(source, source_states, context, replica)
            if to_console:
                print(msg)
            else:
                self.logger.info(msg)
        if to_console:
            print()


    # replica staleness: mtime (status) > mtime (newest file) -or-
    #     mtime(status) > 24h 
    def replica_is_current(self, source, source_states, replica_file):
        # find newest file on source
        mtime = 0
        for fqde, source_state in source_states.items():
            mtime = max(mtime, source_state["mtime"])
        # compare to mtime of replica record
        filestat = os.lstat(replica_file)
        # print(f"Checking currentness: {replica_file} = {filestat.st_mtime}, 24hago {time.time() - 24*60*60}, diff {mtime - (time.time() - 24*60*60)}")
        if (mtime > filestat.st_mtime) or (filestat.st_mtime < time.time() - 24*60*60):
            # TODO: how stale is it?
            return False
        else:
            return True


    def inspect_replica(self, source, source_states, context, replica):
        prefix = f"{config.path_for(source)}/.ghetto_cluster/"
        hostname = config.host_for(replica)
        replica_file = f"{prefix}/{hostname}.{context}.json"
        replica_states = persistent_dict.PersistentDict(replica_file)
        if self.config.getOption("verbose", "False") == "True":
            msg = f"{replica} ::\n"
            missing = mismatch = extra = 0
            lines = 0
            for fqde, source_state in source_states.items():
                if replica_states.contains_p(fqde):
                    replica_states.touch(fqde)
                    replica_state = replica_states.get(fqde)
                    if source_state["size"] != replica_state["size"]:
                        mismatch += 1
                        if lines < 10:
                            msg += f"\tmismatch: {fqde} "
                            if replica_state["ctime"] > source_state["ctime"]:
                                msg += "replica is newer"
                            else:
                                # TODO: tell how stale it is
                                msg += f"{duration_to_str(source_state['ctime'] - replica_state['ctime'])} stale"
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
            extra = len(replica_states.clean_keys())
            if missing + mismatch + extra != 0:
                pct = 100 * len(replica_states.items()) / len(source_states.items())
                if pct > 100:
                    pct = 100
                if int(pct) == 100:
                    pct = 99
                msg += f"\tmissing: {missing} ({pct:.0f}% complete); " + \
                        f"mismatched: {mismatch}; " + \
                        f"extra: {extra}" 
            else:
                # TODO: staleness report
                # msg = self.check_replica_staleness(source, source_states, context, replica) + msg[:-4]
                msg = "Complete: " + msg[:-4]
        else:
            (target_files, target_bytes) = \
                self.sizeof(source_states)
            (nlines, nbytes) = \
                self.sizeof(replica_states)
            pct_complete = int(100*nlines/target_files)
            if nlines == target_files:
                # msg = self.check_replica_staleness(source, source_states, replica_file) + f": {replica}"
                if self.replica_is_current(source, source_states, replica_file):
                    msg = f"  Complete : {replica}"
                else:
                    msg = f"  Stale: {replica}"
            elif nlines == 0:
                msg = f"  Not started: {replica}"
            elif nlines < target_files:
                if self.replica_is_current(source, source_states, replica_file):
                    msg = "  Active: "
                else:
                    msg = "  Stale: "
                msg += f"{pct_complete:3d}% {nlines}/{target_files}: {replica}"
            else:
                msg += f"WARNING: too many files in replica " + \
                        f"{config.host_for(replica)}\n" + \
                        f"\t{nlines}/{target_files}: {replica}"
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
            cfg.setOption("dryrun", "True")
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
            cfg.setOption("testing", "True")
        elif opt == "-v":
            cfg.setOption("verbose", "True")
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
        LOGFILE = os.path.expanduser(cfg.getOption("LOGFILE", "gc.log"))
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
    # gcm = GhettoClusterSource("/Volumes/Media_ZFS/Movies/action")
    # gcm.scan()
