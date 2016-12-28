#!/usr/bin/env python

import os
import sys
import time
import requests
import argparse
import subprocess

# what if this script fails somewhere.
# say the nsq doesn't connect, etc.
# we have to use local logging for this.
import logging
logging.basicConfig(
    stream=sys.stderr,
    datefmt="%Y-%m-%dT%H-%M-%S",
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger()

class ProcessWrapper(object):
    _NSQD_HTTP_ADDRESS = "http://127.0.0.1:4151"
    _NSQ_LOG_TOPIC = "log"
    _DISABLE_NSQ = False
    _PID_FILE = None

    def __init__(
            self,
            command,
            pid_file=_PID_FILE,
            disable_nsq=_DISABLE_NSQ,
            nsq_log_topic=_NSQ_LOG_TOPIC,
            nsqd_http_address=_NSQD_HTTP_ADDRESS):

        self.command = command
        self.pid_file = pid_file
        self.disable_nsq = disable_nsq
        self.nsq_log_topic = nsq_log_topic

        nsqd_http_address = nsqd_http_address.rstrip('/')
        if not nsqd_http_address.startswith("http://"):
            nsqd_http_address = 'http://%s' % nsqd_http_address

        self.nsqd_http_address = nsqd_http_address

        self.process = None
        self.write_to_nsq = lambda msg: None

        if not self.disable_nsq:
            self.init_nsq()

    def init_nsq(self):
        session = requests.Session()
        # TODO use urljoin ?
        response = None
        try:
            response = session.get("%s/ping" % self.nsqd_http_address)
            if response.status_code != 200:
                raise Exception("bad response %s" % response)
        except:
            logger.exception(
                "if you wish to disable nsqd, use --disable-nsq"
            )
            sys.exit(-1)

        # TODO nsq is a failure point for our logs. what do we do if nsq is down ?

        url = "%s/pub" % self.nsqd_http_address
        topic = self.nsq_log_topic
        # TODO what if this write to nsq fails ?
        self.write_to_nsq = lambda msg: session.post(url, data=msg, params={"topic": topic})

    def start(self):
        """
        starts the command and blocks till it finishes.
        once finished, returns the returncode.
        """

        if self.pid_file is not None:
            d = os.path.dirname(self.pid_file)
            if not os.path.exists(d):
                os.makedirs(d)

        # NOTE only capturing stderr. stdout still goes to tty.

        self.process = subprocess.Popen(
            self.command, bufsize=1, universal_newlines=True,
            stderr=subprocess.PIPE,
        )

        if self.pid_file is not None:
            with open(self.pid_file, "w") as fp:
                fp.write("%s" % self.process.pid)

        read_stderr_line = self.process.stderr.readline
        poll = self.process.poll
        # TODO buffering and sending many to nsq together ?
        # TODO avoid
        while poll() is None:
            line = read_stderr_line()
            # TODO pretty print to stderr ?
            sys.stderr.write(line)

            # the line may not be valid json object
            if line.startswith("{") and line.endswith("}\n"):
                # TODO load the json line, add some other parameters like hostname, uuid, etc
                # serialize again and send to nsq
                # TODO check that it is a log

                # NOTE for now just sending it across
                self.write_to_nsq(line)

            # FIXME is this a bottleneck ? only one log per 0.1 seconds ?
            # consider threads http://eyalarubas.com/python-subproc-nonblock.html
            time.sleep(0.1)

        # read the remaining stderr
        remaining = self.process.stderr.read()
        sys.stderr.write(remaining)
        sys.stderr.flush()

        return self.process.returncode

def parse_args():
    # TODO accept configuration file

    parser = argparse.ArgumentParser(
        description="process wrapper",
    )

    parser.add_argument("-p", "--pid",
        default=ProcessWrapper._PID_FILE,
        help="write the pid to file if present, default: %(default)s"
    )

    # TODO should this be the other way around ? always disabled ?
    parser.add_argument("--disable-nsq", action="store_true",
        default=ProcessWrapper._DISABLE_NSQ,
        help="to disable pushing to nsq, default: %(default)s",
    )

    parser.add_argument("--nsq-log-topic",
        default=ProcessWrapper._NSQ_LOG_TOPIC,
        help="nsq topic name for logs, default: %(default)s",
    )

    parser.add_argument("--nsqd-http-address",
        default=ProcessWrapper._NSQD_HTTP_ADDRESS,
        help="nsqd http address, default: %(default)s",
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
    procwrap = ProcessWrapper(
        args.command,
        pid_file=args.pid,
        disable_nsq=args.disable_nsq,
        nsq_log_topic=args.nsq_log_topic,
        nsqd_http_address=args.nsqd_http_address,
    )
    returncode = procwrap.start()
    sys.exit(returncode)

if __name__ == '__main__':
    main()
