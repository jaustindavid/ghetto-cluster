#! python3

# <class '__main__.GhettoCluster'> -> GhettoCluster
def logger_str(classname):
    string = str(classname)
    return string[string.index('.')+1:string.index("'>")]
