#! python3.x

import sys, localState, config

def main():
    pass

def ui_help():
    print("""
scan <director>
    scans <directory> for changes

settings
    shows all settings

peer 
    not implemented :(

exit | quit | ^D
    does what you think
""")

def ui_scan(argv):
    localState.scandir(str(argv[0]))

def ui_settings(argv):
    c = config.Config.instance()
    print(c.config)

def ui_peer(argv):
    print("Not yet implemented :(")

def ui_exit(argv = list()):
    sys.exit()

commands = { 'scan'     : ui_scan,
             'settings' : ui_settings,
             'peer'     : ui_peer,
             'exit'     : ui_exit,
             'quit'     : ui_exit
            }

if __name__ == "__main__":
    print("doing the thing?")
    c = config.Config.instance()
    c.init("config.txt")
    while True:
        try:
            command = input(">")
        except (EOFError, KeyboardInterrupt):
            print("Exiting...")
            ui_exit()
        print(f"command: {command}")
        argv = command.split(" ")
        print(argv)
        if argv[0] in commands:
            commands[argv[0]](argv[1:])
        else:
            ui_help()
