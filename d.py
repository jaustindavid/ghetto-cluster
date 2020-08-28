import time, daemonize, logging, logging.handlers

def main():
    logger = logging.getLogger(__file__)
    while True:
        logger.info("yup")
        time.sleep(5)

logger = logging.getLogger(__name__)
# logger.propagate = False
fh = logging.FileHandler("d.log", "a")
fh = logging.handlers.RotatingFileHandler( \
        "d.log", maxBytes=10*2**20, backupCount=5)
fh.setLevel(logging.DEBUG)
# logger.addHandler(fh)
logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', 
                    # handlers=[fh], level=logging.DEBUG)
                    level=logging.DEBUG)
logger.info("logging")

keep_fds = [fh.stream.fileno()]
daemon = daemonize.Daemonize(app="test", pid="/tmp/app.pid", \
                            action=main, keep_fds=keep_fds)
daemon.start()
