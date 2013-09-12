import sys
import onewired

if __name__ == "__main__":
    if len(sys.argv) == 2:
        owd = onewired.OneWireDaemon(sys.argv[1])
        owd.start()
    else:
        print "I'll need a path to find the config file..."
        sys.exit(1)
