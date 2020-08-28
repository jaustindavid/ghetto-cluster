#!/usr/bin/env python3

import logging, os, subprocess, re, sys

def rsync(source, dest, options = [], **kwargs):
    logger = logging.getLogger("gc.rsync")

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
    command = [ RSYNC ] + options
    command.append(doctored_source)
    command.append(doctored_dest)
    logger.debug(f">> {command}")

    # subprocess.call(command)
    # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging#comment33261012_21953835
    from subprocess import Popen, PIPE, STDOUT
    if "stfu" in kwargs and kwargs["stfu"]:
    	loghole = logger.debug
    else:
        loghole = logger.info
    try:
        logger.debug(f"running: {command}")
        process = Popen(command, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                # b'\n'-separated lines
                # logger.info("> %s", line.decode().strip())
                loghole(f"> {line.decode().strip()}")
                # print(f"> {line.decode().strip()}")
            exitcode = process.wait()
    except BaseException:
        process.terminate()
        logger.exception("Caught ...something")
        sys.exit()


if __name__ == "__main__":
    logger = logging.getLogger('gc')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    rsync("bucko:/var/log/dmesg", "/tmp", [ "-v" ])
