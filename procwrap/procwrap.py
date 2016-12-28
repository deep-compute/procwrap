#!/usr/bin/env python

import os
import sys
import argparse
import subprocess

def procwrap(command, pid_file=None):
    """
    executes the process specified in args.command.
    returns the returncode of the command.
    """
    if pid_file is not None:
        d = os.path.dirname(pid_file)
        if not os.path.exists(d):
            os.makedirs(d)

    process = subprocess.Popen(command)
    if pid_file is not None:
        with open(pid_file, "w") as fp:
            fp.write("%s" % process.pid)

    process.wait()
    return process.returncode

def parse_args():
    parser = argparse.ArgumentParser(
        description="process wrapper",
    )

    parser.add_argument("-p", "--pid",
        help="write the pid to file if present"
    )

    parser.add_argument("command", nargs=argparse.REMAINDER,
        help="the command to run",
    )
    args = parser.parse_args()
    if args.pid:
        args.pid = os.path.abspath(os.path.expanduser(args.pid))

    return args

def main():
    args = parse_args()
    returncode = procwrap(args.command, pid_file=args.pid)
    sys.exit(returncode)

if __name__ == '__main__':
    main()
