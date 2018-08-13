#! python3.x

"""
usage:
    from elapsed import ElapsedTimer
    ...
    timer = ElapsedTimer()
    while True:
        print(f"It's been {timer.elapsed()}s since start")
        if timer.once_every(10):
            print("This happens only once every 10s")
        time.sleep(1)

    ...
    timer.reset() 
    print(f"0: {int(timer.elapsed())}; True: {timer.once_every(5)}")
    print(f"0: {int(timer.elapsed())}; False: {timer.once_every(5)}")
    time.sleep(5)
    print(f"5: {int(timer.elapsed())}; True: {timer.once_every(5)}")
"""

import time

class ElapsedTimer:
    def __init__(self):
        self.reset()

    def elapsed(self):
        return time.time() - self.start

    def reset(self):
        self.start = time.time()
        self.last = 0

    def once_every(self, interval):
        if (self.last + interval) < time.time():
            self.last = time.time()
            return True
        return False


if __name__ == "__main__":
    from elapsed import ElapsedTimer

    timer = ElapsedTimer()
    while timer.elapsed() < 5:
        print(f"It's been {timer.elapsed():3.1f}s since start")
        if timer.once_every(2):
            print("This happens only once every 2s")
        time.sleep(1)
