import logging, time
from daemonize import Daemonize

pid = "/tmp/test.pid"
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(message)s')
logger.setLevel(logging.DEBUG)
logger.info("starting up")
logger.propagate = False
fh = logging.FileHandler("/tmp/test.log", "a")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
keep_fds = [fh.stream.fileno()]


def main():
    for i in range(0,10):
        logger.debug(f"Test {i}")
        time.sleep(5)

daemon = Daemonize(app="test_app", pid=pid, action=main, keep_fds=keep_fds)
daemon.start()
