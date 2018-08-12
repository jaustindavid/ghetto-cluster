#! python3.6

import os, json, time, hashlib, random, config


# { 'name' : filename, 
#   'size': int, 
#   'checksum' : sha256, 
#   'checksum_time' : time_t,
#   'ctime' : time_t,
#   'mtime' : time_t }
class FileState:
    def __init__(self, filename, genChecksums = False):
        self.data = {'filename' : filename}
        self.update(genChecksums)


    def update(self, genChecksums = False):
        cfg = config.Config.instance()
        BLOCKSIZE = int(cfg.getConfig("global", "BLOCKSIZE", 2**10))
        NBLOCKS = int(cfg.getConfig("global", "NBLOCKS", 10))
        
        if genChecksums:
            self.data['checksum'] = \
                sum_sha256(self.data['filename'], BLOCKSIZE, NBLOCKS)
        else:
            self.data['checksum'] = 'deferred'
        self.data['checksum_time'] = time.time()
        try:
            filestat = os.lstat(self.data['filename'])
            self.data['size'] = filestat.st_size
            self.data['ctime'] = filestat.st_ctime
            self.data['mtime'] = filestat.st_mtime
        except FileNotFoundError:
            self.data['checksum'] = 'n/a'
            self.data['size'] = 0
            self.data['ctime'] = 0
            self.data['mtime'] = 0


    def from_dict(self, data):
        self.data = data


    def to_dict(self):
        return self.data


    def maybechanged(self, filestate_data):
        return self.data['ctime'] != filestate_data['ctime'] \
            or self.data['mtime'] != filestate_data['mtime'] \
            or self.data['size'] != filestate_data['size']
            

    def changed(self, filestate_data):
        if self.data['checksum'] == filestate_data['checksum']:
            return False
        return True


    def __str__(self):
        return str(self.data)
        return f"{self.filename}[{self.size}b]: "\
               f"{self.checksum[0-2]}..{self.checksum[-2:]}"\
               f"@{self.checksum_time} c:{self.ctime} m:{self.mtime}"



# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
# https://gist.github.com/aunyks/042c2798383f016939c40aa1be4f4aaf
def sum_sha256(fname, BLOCKSIZE = 2**20, NBLOCKS = 10):
    if not os.path.isfile(fname):
        return None
    hash_sha256 = hashlib.sha256()
    filestat = os.lstat(fname)
    with open(fname, "rb") as f:
        # for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
        #    hash_sha256.update(chunk)
        if filestat.st_size < NBLOCKS*BLOCKSIZE:
            # "small" files, 10MB or less
            file_buffer = f.read(BLOCKSIZE)
            while len(file_buffer) > 0:
                hash_sha256.update(file_buffer)
                file_buffer = f.read(BLOCKSIZE)
        else:
            # "large" files > 10MB; randomly sample (up to) 10 blocks
            file_buffer = f.read(BLOCKSIZE)
            random.seed(filestat.st_size)
            count = 0
            step = int(filestat.st_size/NBLOCKS)
            jump = random.randrange(step)
            f.seek(jump)
            # print(f"size: {filestat.st_size} step: {step} jump: {jump}")
            while len(file_buffer) > 0: # and count < NBLOCKS:
                # print(f"So far @ {count}:{f.tell()}: {hash_sha256.hexdigest()}")
                hash_sha256.update(file_buffer)
                file_buffer = f.read(BLOCKSIZE)
                f.seek(step, 1)
                count += 1
    return hash_sha256.hexdigest()


def scandir(path, verbose = False):
    try:
        entries = os.listdir(path)
    except FileNotFoundError:
        return {"__total__": 0}

    total_size = 0
    entry_dict = {"__total__": 0}
    for entry in entries:
        filename = f"{path}/{entry}"
        filestat = os.lstat(filename)
        if verbose:
            print(filestat)
        total_size += filestat.st_size
        checksum = sum_sha256(filename)
        print(f"{filename} : {checksum}")
        entry_dict[filename] = {"size": filestat.st_size,
                                "checksum": checksum,
                                "checsum_time" : time.time(),
                                "ctime": filestat.st_ctime,
                                "mtime": filestat.st_mtime }
        entry_dict["__total__"] += filestat.st_size
    if verbose:
        print(total_size)
    return entry_dict

# cleans up the entries defined in entry_dict (obtained via scandir)
# removing the LRU (based on mtime) while projected __total__ > target_size
def lru_files_to_size_target(entry_dict, target_size = 0, verbose = False):
    if verbose:
        print(f"Target: {target_size}")
    projected_total = entry_dict["__total__"]
    del entry_dict["__total__"]
    targets = []
    for entry in sorted(list(entry_dict), key=lambda k: \
                            entry_dict[k].st_mtime):
        if projected_total > target_size:
            targets.append(entry)
            projected_total -= entry_dict[entry].st_size
            if verbose:
                print(f"removing {entry} ({entry_dict[entry].st_size}; "
                      f"{projected_total - target_size} to go")
        else:
            break
    return targets


def cleanup(targets, verbose = False):
    for filename in targets:
        if verbose:
            print(f"Removing {filename}")
        os.remove(filename)


# usage:
# entries = scandir("path/to/directory")
# filenames = lru_files_to_size_target(entries)
# cleanup(filenames)

if __name__ == "__main__":
    fs = FileState("localState.py")
    print(fs)
