#! python3

# <class '__main__.GhettoCluster'> -> GhettoCluster
def logger_str(classname):
    string = str(classname)
    return string[string.index('.')+1:string.index("'>")]


# 64 -> "1m4s"
def duration_to_str(seconds):
    string = ""
    if seconds > 24*60*60:
        days = int(seconds / (24*60*60))
        seconds = seconds % (24*60*60)
        string += f"{days}d"
    if seconds > 60*60:
        hours = int(seconds / (60*60))
        seconds = seconds % (60*60)
        string += f"{hours}h"
    if seconds > 60:
        minutes = int(seconds / 60)
        seconds = seconds % 60
        string += f"{minutes}m"
    if seconds > 0:
        string += f"{int(seconds)}s"
    return string


# "6h1s" -> 21601
# method is really simplistic: ##d -> ## * (24 hrs); ##s -> ##
#   ... iteratively walk the string, scaling ## by d/h/m/s
# side effect: other characters are ignored, 1z6 -> 16 (seconds)
def str_to_duration(string):
    nr = 0
    total = 0
    while string is not "":
        if string[0].isdigit():
            nr = nr*10 + int(string[0])
        elif string[0] == "d":
            total += 24*3600 * nr
            nr = 0
        elif string[0] == "h":
            total += 3600 * nr
            nr = 0
        elif string[0] == "m":
            total += 60 * nr
            nr = 0
        elif string[0] == "s":
            total += nr
            nr = 0
        string = string[1:]
    total += nr
    return total



if __name__ == "__main__":
    import random
    for i in range(3):
        s = random.randrange(1000000)
        print(f"{s}s -> {duration_to_str(s)}")
