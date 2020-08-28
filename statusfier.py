#!/usr/bin/env python3.7

import logging, os, os.path, time
import persistent_dict, config
from utils import str_to_duration, duration_to_str

def sizeof(states):
    nfiles = nbytes = 0
    for fqde, state in states.items():
        nfiles += 1
        nbytes += state["size"]
    return (nfiles, nbytes)


def state_filename(context, path_src, hostname_src):
    path = f"{config.path_for(path_src)}/.gc"
    hostname = config.host_for(hostname_src)
    return f"{path}/{hostname}.{context}.json"


class Statusfier:
    def __init__(self):
        self.logger = logging.getLogger("gc.Statusfier")
        self.config = config.Config.instance()


    def newest_checksum(self, states):
        # find newest checksum_time
        checksum_time = 0
        for fqde, state in states.items():
            checksum_time = max(checksum_time, state["checksum_time"])
        return checksum_time


    def state_latency(self, states):
        latency = time.time() - self.newest_checksum(states)
        if latency < str_to_duration(self.config.getOption("cycle", "24h")):
            msg = "Current"
        else:
            msg = "Stale"
        msg += f"; last update {duration_to_str(latency)} ago"
        return msg


    # TODO: report staleness in the source
    # returns a line of text, ex:
    # Source: mini-pi:/Volumes/Media_ZFS/Movies: 1601 files, 1248.87GB
    def inspect_source(self, context, source, source_states):
        (files, bytes) = sizeof(source_states)
        return f"{source}: {files} files, {bytes/2**30:.2f}GB" \
                + "\n\t" + self.state_latency(source_states)



    # returns newest checksum_time in source - newest checksum_time in replica
    def replica_latency(self, source_states, replica_states):
        return self.newest_checksum(source_states) - \
                self.newest_checksum(replica_states)


    def replica_is_current(self, latency):
        return latency < str_to_duration(self.config.getOption("cycle", "24h"))


    # returns a line or two of text, ex:
    #  Replica bucko:/mnt/docs/Family Stuff.sparsebundle: 100% 81961/81961
    #         Current; last update 2h20m31s ago
    def inspect_replica(self, replica, source_states, replica_states):
        msg = f"{replica}: "
        if len(replica_states.items()) == 0:
            return msg + "Not started" + "\n\tNo data"
        (target_files, target_bytes) = sizeof(source_states)
        if target_files == 0:
            return msg + "no data in source"
        (replica_files, replica_bytes) = sizeof(replica_states)
        pct_complete = int(100*replica_files/target_files)
        msg += f"{pct_complete:3d}% {replica_files}/{target_files}, " \
                + f"{replica_bytes/2**30:.2f}GB"
        latency = self.replica_latency(source_states, replica_states)
        return msg + "\n\t" + self.state_latency(replica_states)
        if replica_files == target_files:
            if self.replica_is_current(latency):
                pass
            else:
                return f"Stale @ 100%: {replica}; {duration_to_str(latency)} ago"
        if replica_files < target_files:
            if self.replica_is_current(latency):
                msg = "Active: "
            else:
                msg = "Stale: "
            msg += f"{pct_complete:3d}% {replica_files}/{target_files}: {replica}"
            return msg
        return f"WARNING: too many files in replica " \
                + f"{config.host_for(replica)}\n" \
                + f"\t{replica_files}/{target_files}: {replica}"


    def get_status_for_source(self, context, source):
        source_file = state_filename(context, source, source)
        source_states = persistent_dict.PersistentDict(source_file)
        self.logger.info(self.inspect_source(context, source, source_states))
        for replica in self.config.get_replicas_for_context(context):
            replica_file = state_filename(context, source, replica)
            replica_states = persistent_dict.PersistentDict(replica_file)
            msg = self.inspect_replica(replica, source_states, replica_states)
            self.logger.info(f"  {msg}")
    

    def get_status_for_replica(self, context, replica):
        source = self.config.get_source_for_context(context)
        source_file = state_filename(context, replica, source)
        source_states = persistent_dict.PersistentDict(source_file)
        replica_file = state_filename(context, replica, replica)
        self.logger.debug(f"replica file: {replica_file}")
        replica_states = persistent_dict.PersistentDict(replica_file)
        msg = self.inspect_replica(replica, source_states, replica_states)
        self.logger.info(msg)
        (files, bytes) = sizeof(source_states)
        self.logger.info(f"  Source: {source}: " \
                + f"{files} files, {bytes/2**30:.2f}GB")

