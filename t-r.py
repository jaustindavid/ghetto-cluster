#! python3.x

import random, localState

size = random.randrange(50,100)
print(size)

i = 0
count = 0
step = int(size/10)
print(f"step: {step}")
while i < size:
    i = random.randrange(int(count * step), int((count+1)*step))
    print(i, count)
    count += 1

print(localState.sum_sha256("100mb"))
