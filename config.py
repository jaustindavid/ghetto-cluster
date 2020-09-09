#! python3.x

import platform, os, re, logging, sys
from singleton import Singleton
from rsync import rsync


"""
Usage:
config.txt:
    source config: mini-pi:~/gc/config.txt

    ignore suffix: .DS_Store, .fseventsd, .Spotlight-V100, .DocumentRevisions-V100, .sync
    options: LAZY WRITE: 10, RSYNC TIMEOUT: 180, CYCLE: 21600, LOGFILE: ~/gc/gc.log

    source: mini-pi:/Volumes/Media_ZFS/Movies
    replica: mc-wifi:/mnt/disk1/Movies
    ignore suffix: mobile, index.html, streams

#! python
  cfg = config.Config.instance()
  # ... ONCE on startup: 
  cfg.init(filename)
  periodically:
    cfg.load()


Theory of Operation:
    Config holds context:key:value settings; "global" is context 0, 
        "local" contexts are numbered serially 1..N.  There is no 
        nesting of contexts.
            setConfig(context, key, value)
            getConfig(context, key) => value

    Config holds "source" and "replica" contexts:
        get_sources_for_host(hostname) -> { context : source }
        get_replicas_for_host(hostname) -> { context : replica }
        get_source_for_context(context) -> "source"
        get_replicas_for_context(context) -> [ replicas ]

    A source would iterate over get_sources_for_host(hostname),
        then for each source get_replicas_for_context(context)
    A replica would iterate over get_replicas_for_host(hostname), 
        then for each replica get_source_for_context(context) 

"""

@Singleton
class Config:
    def __init__(self):
        self.source_config = None
        self.config = {}
        self.verbose = True
        self.sources = {}
        self.replicas = {}
        self.logger = logging.getLogger("gc.Config")
        self.logger.setLevel(logging.INFO)


    def init(self, filename, hostname = None, testing = False):
        if not filename.startswith("/"):
            filename = os.getcwd() + "/" + filename
        self.filename = filename
        if hostname is None:
            self.hostname = platform.node()
        else:
            self.hostname = hostname
        self.testing = testing
        # prime the config (can't pull yet)
        self.read_config()
        # load for realz
        self.load()
        self.verbose = self.getConfig(0, "verbose", False)


    # if testing, just copy the thing (rsync w/o hostnames)
    #   otherwise, rsync to the config filename
    def pull_source_config(self):
        if self.source_config is None:
            self.logger.error(f"ERROR: {self.filename} MUST contain a " \
                             f"\"source config:\" line")
            sys.exit(1)
        host = host_for(self.source_config)
        if host == self.hostname:
            self.logger.debug("I am source, not pulling the config")
            return
        self.logger.debug(f"pulling config {self.source_config}" \
                         f" -> {self.filename}")
        if self.testing:
            self.logger.debug(self.source_config)
            source_config = path_for(self.source_config)
        else:
            source_config = self.source_config
        rsync(source_config, self.filename, stfu=True)


    def read_config(self):
        self.logger.debug("Loading the config")
        source = None
        context = 0
        try:
            with open(self.filename, "r") as file:
                for line in file:
                    tokens = line.strip().split(": ", 1)
                    self.logger.debug(f"> {line.strip()}") 
                    if tokens[0] == "source config" and source is None:
                        self.source_config = tokens[1]
                        # fallthrough to tokens...
                        # self.config[0] = {}
                        # self.config[0]["source config"] = self.source_config
                    if tokens[0] == "source":
                        source = tokens[1]
                        context += 1
                        self.config[context] = {}
                        self.config[context]["source"] = source
                        self.config[context]["replicas"] = []
                        # self.sources[context] = source
                        # self.replicas[context] = []
                    elif tokens[0] == "replica":
                        replica = tokens[1]
                        # self.data[source].append(replica)
                        self.config[context]["replicas"].append(replica)
                        # self.replicas[context].append(replica)
                    elif len(tokens) == 2:
                        # not a "source" or "replica", must be a config option
                        # self.setConfig(source, tokens[0], tokens[1])
                        self.setConfig(context, tokens[0], tokens[1])
            self.logger.debug(self.config)
        except BaseException:
            self.logger.exception("Fatal error reading config")
            self.logger.error(f"Confirm {self.filename} is readable")
            sys.exit(1)
        # self.process_global_options()


    def load(self):
        if not self.testing:
            self.pull_source_config()
        self.read_config()

    
    # explodes source config\n options: KEY: VALUE, KEY2: VALUE2
    #   into { 0: { key: value, key2: value2 } }
    def process_global_options(self):
        options = self.getConfig(0, "options") 
        if options is None:
            return
        for option in options:
            # "LAZY WRITE: 10" -> key, value
            tokens = option.split(": ")
            self.setConfig(0, tokens[0], tokens[1])
            self.logger.debug(f"global option {tokens[0]} => {tokens[1]}")


    # a simplified wrapper for getConfig(0, option)
    def getOption(self, option, default=None):
        return self.getConfig(0, option, default)[0]


    # a simplified wrapper for setConfig(0, option, value)
    def setOption(self, option, value):
        return self.setConfig(0, option, value)


    def get_sources_for_host(self, hostname):
        sources = {}
        for context in self.config.keys():
            if "source" in self.config[context] \
                and "False" in self.getConfig(context, 'disabled', 'False'):
                if host_for(self.config[context]["source"]) == hostname:
                    sources[context] = self.config[context]["source"]
        return sources


    def get_replicas_for_host(self, hostname):
        replicas = {}
        for context in self.config.keys():
            if "replicas" in self.config[context] \
                and "False" in self.getConfig(context, 'disabled', 'False'):
                for replica in self.config[context]["replicas"]:
                    if host_for(replica) == hostname:
                        replicas[context] = replica
        return replicas


    def get_source_for_context(self, context):
        # return self.sources[context]
        return self.config[context]["source"]


    def get_replicas_for_context(self, context):
        # return self.replicas[context]
        return self.config[context]["replicas"]


    def get_dirs(self, hostname = None):
        sources = []
        replicas = {}
        if hostname == None:
            hostname = self.hostname
        for key, values in self.data.items():
            if key.startswith(hostname):
                sources.append(key)
            else:
                for value in values:
                    if value.startswith(hostname):
                        replicas[value] = key
        return sources, replicas


    def get_ignorals(self, context):
        ignorals = [] # ".gc" ] # actually we should ship this around
        if "ignore suffix" in self.config[0]:
            ignorals += self.getConfig(0, "ignore suffix")
        # print(f"ignorals w/ global: {ignorals}")
        if "ignore suffix" in self.config[context]:
            ignorals += self.getConfig(context, "ignore suffix")
        # print(f"ignorals + {context}: {ignorals}")
        return ignorals


    def setConfig(self, context, key, value):
        self.logger.debug(f"setting {context}: {key} => {value}")
        if context == None:
            context = 0
        if context not in self.config:
            self.config[context] = {}
        self.config[context][key] = value.split(", ")

            
    def getConfig(self, context, key, default=None, follow=True):
        if context in self.config:
            if key in self.config[context]:
                return self.config[context][key]
            if follow and key in self.config[0]:
                return self.config[0][key]
        # print(f"Failed to find {context}=>{key}")
        return [default]



def host_for(host_path):
    return host_path.split(":")[0]

def path_for(host_path):
    return host_path.split(":")[1]
