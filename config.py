#! python3.x

import platform, os, re, file_state, logging, sys
from singleton import Singleton
from utils import logger_str


"""
Usage:
  cfg = config.Config.instance()
   ... ONCE on startup: cfg.init(filename)
  periodically:
    cfg.load()


Theory of Operation:
    Config holds context:key:value settings, including a 
        "global" context (getConfig, setConfig)
        setConfig(context, key, value)
        getConfig(context, key) => value

    Config holds "master" and "slave" contexts:
        get_masters_for_host(hostname) -> { context : master }
        get_slaves_for_host(hostname) -> { context : slave }
        get_master_for_context(context) -> "master"
        get_slaves_for_context(context) -> [ slaves ]

    A master would iterate over get_masters_for_host(hostname),
        then for each master get_slaves_for_context(context)
    A slave would iterate over get_slaves_for_host(hostname), 
        then for each slave get_master_for_context(context) 

"""

@Singleton
class Config:
    def __init__(self):
        self.master_config = None
        self.data = {}
        self.config = {}
        self.verbose = False
        self.masters = {}
        self.slaves = {}
        self.logger = logging.getLogger(logger_str(__class__))


    def init(self, filename, hostname = None, testing = False):
        self.filename = filename
        if hostname is None:
            self.hostname = platform.node()
        else:
            self.hostname = hostname
        self.testing = testing
        self.verbose = self.getConfig("global", "verbose", False)
        # prime the config (can't pull yet)
        self.read_config()
        # load for realz
        self.load()


    def DEADread_local_config(self, verbose = False):
        with open(self.filename, "r") as file:
            for line in file:
                if verbose:
                    print(line.strip()) 
                tokens = line.strip().split(": ", 1)
                if tokens[0] == "hostname":
                    self.hostname = tokens[1]
                elif tokens[0] == "master config":
                    self.master_config = tokens[1]
        self.pull_master_config(True)


    # if testing, just copy the thing (rsync w/o hostnames)
    #   otherwise, rsync to the config filename
    def pull_master_config(self):
        if self.master_config is None:
            self.logger.error(f"ERROR: {self.filename} MUST contain a " \
                             f"\"master config:\" line")
            sys.exit(1)
        host = host_for(self.master_config)
        if host == self.hostname:
            self.logger.debug("I am master, not pulling the config")
            return
        self.logger.info(f"pulling config {self.master_config}" \
                         f" -> {self.filename}")
        if self.testing:
            self.logger.debug(self.master_config)
            master_config = path_for(self.master_config)
        else:
            master_config = self.master_config
        file_state.rsync(master_config, self.filename)


    def read_config(self):
        self.logger.debug("Loading the config")
        master = None
        context = 0
        try:
            with open(self.filename, "r") as file:
                for line in file:
                    tokens = line.strip().split(": ", 1)
                    self.logger.debug(f"> {line.strip()}") 
                    if tokens[0] == "master config" and master is None:
                        self.master_config = tokens[1]
                    if tokens[0] == "master":
                        master = tokens[1]
                        context += 1
                        self.data[master] = []
                        self.masters[context] = master
                        self.slaves[context] = []
                    elif tokens[0] == "slave":
                        slave = tokens[1]
                        self.data[master].append(slave)
                        self.slaves[context].append(slave)
                    elif len(tokens) == 2:
                        # not a "master" or "slave", must be a config option
                        self.setConfig(master, tokens[0], tokens[1])
                        self.setConfig(context, tokens[0], tokens[1])
            self.logger.debug(self.data)
        except BaseException:
            self.logger.exception("Fatal error reading config")
            self.logger.error(f"Confirm {self.filename} is readable")
            sys.exit(1)
        self.process_global_options()


    def load(self):
        self.pull_master_config()
        self.read_config()

    
    def process_global_options(self):
        options = self.getConfig("global", "options") 
        if type(options) is str:
            self.process_option("global", options)
        else:
            for option in options:
                self.process_option("global", option)


    # "LAZY WRITE: 10" -> key, value
    def process_option(self, context, option):
        tokens = option.split(": ")
        self.setConfig(context, tokens[0], tokens[1])
        self.logger.debug(f"option {context}:{tokens[0]} => {tokens[1]}")


    def get_masters_for_host(self, hostname):
        masters = {}
        for context, master in self.masters.items():
            if host_for(master) == hostname:
                masters[context] = master
        return masters


    def get_slaves_for_host(self, hostname):
        slaves = {}
        for context, slave_list in self.slaves.items():
            for slave in slave_list:
                if host_for(slave) == hostname:
                    slaves[context] = slave
        return slaves


    def get_master_for_context(self, context):
        return self.masters[context]


    def get_slaves_for_context(self, context):
        return self.slaves[context]


    def get_dirs(self, hostname = None):
        masters = []
        slaves = {}
        if hostname == None:
            hostname = self.hostname
        for key, values in self.data.items():
            if key.startswith(hostname):
                masters.append(key)
            else:
                for value in values:
                    if value.startswith(hostname):
                        slaves[value] = key
        return masters, slaves


    def setConfig(self, context, key, value):
        self.logger.debug(f"setting {context}:{key} => {value}")
        if context == None:
            context = 'global'
        if context not in self.config:
            self.config[context] = {}
        if type(value) is str and ", " in value:
            self.config[context][key] = value.split(", ")
        else:
            self.config[context][key] = value

            
    def getConfig(self, context, key, default=None):
        if context in self.config:
            if key in self.config[context]:
                return self.config[context][key]
            if key in self.config["global"]:
                return self.config["global"][key]
        # print(f"Failed to find {context}=>{key}")
        return default



def host_for(host_path):
    return host_path.split(":")[0]

def path_for(host_path):
    return host_path.split(":")[1]
