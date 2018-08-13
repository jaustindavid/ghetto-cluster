#! python3.6

import logging, os, json, time, hashlib, random, subprocess, re
import config


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
        BLOCKSIZE = int(cfg.getConfig("global", "BLOCKSIZE", 2**20))
        NBLOCKS = int(cfg.getConfig("global", "NBLOCKS", 0))
        
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
#
# NBLOCKS & BLOCKSIZE > 0: sampling
#  randomly (seeded on filesize, so consistent) sample
#  NBLOCKS of BLOCKSIZE in the file.  This should make
#  for ~ constant time hashing of very large files
#  sent NBLOCKS or BLOCKSIZE to 0 to disable
#
# For tuning to an FS, set NBLOCKS to 0 (no sampling) and
#  BLOCKSIZE to an integer multiple of the FS chunk size
def sum_sha256(fname, BLOCKSIZE = 2**20, NBLOCKS = 0):
    if not os.path.isfile(fname):
        return None
    hash_sha256 = hashlib.sha256()
    filestat = os.lstat(fname)
    with open(fname, "rb") as f:
        # for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
        #    hash_sha256.update(chunk)
        if NBLOCKS*BLOCKSIZE == 0 or filestat.st_size < NBLOCKS*BLOCKSIZE:
            # "small" files, 10MB or less
            if BLOCKSIZE == 0:
                BLOCKSIZE = 2**20 # 1MB
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



def rsync(source, dest, options = []):
    cfg = config.Config.instance()
    verbose = cfg.getConfig("global", "verbose", False)
    dryrun = cfg.getConfig("global", "dryrun", False)

    #jsrc_host = source[:source.index(':')]
    # dest_host = dest[:dest.index(':')]
    if False and src_host == dest_host:
        # source and dest are the same; just copy
        src_file = source[source.index(':') + 1:]
        dest_file = dest[dest.index(':') + 1:]
        command = [ "cp", src_file, dest_file ]
    else:
        # source and dest are not the same; rsync
        RSYNC = "rsync"
	# rsync is silly about escaping spaces -- remote locations ONLY
        if ":" in source:
            doctored_source = re.sub(r' ', '\ ', source)
        else:
            doctored_source = source
        if ":" in dest:
            doctored_dest = re.sub(r' ', '\ ', dest)
        else:
            doctored_dest = dest
        RSYNC_TIMEOUT = str(cfg.getConfig("global", "RSYNC TIMEOUT", 180))
        command = [ RSYNC, "-a", "--inplace", "--partial", \
                    "--timeout", RSYNC_TIMEOUT, \
                    doctored_source, doctored_dest ]
        if len(options) > 0:
            command += options
        # if len(ignorals) > 0:
        #     command += [ f"--exclude={item}" for item in ignorals ] 
        if True or verbose:
            command += ["-v", "--progress"]
    logger = logging.getLogger("rsync")
    logger.debug(command)
    if dryrun:
        logger.info("> " + " ".join(command))
    else:
        # subprocess.call(command)
        # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging#comment33261012_21953835
        from subprocess import Popen, PIPE, STDOUT

        process = Popen(command, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                # b'\n'-separated lines
                logger.info("> %s", line.decode().strip())
            exitcode = process.wait()



if __name__ == "__main__":
    fs = FileState("file_state.py")
    print(fs)
