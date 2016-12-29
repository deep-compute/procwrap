#!/usr/bin/env python

import os
import sys
import time
import json
import atexit
import requests
import argparse
import subprocess

from basescript import BaseScript

class ProcessWrapper(BaseScript):
    DESC = "process wrapper"

    def pre_run_init(self):
        self.write_to_nsq = lambda msg: None
        self.process = None

        if not self.args.enable_nsq:
            return

        addr = self.args.nsqd_http_address
        addr = addr.rstrip('/')
        if not addr.startswith('http://'):
            addr = 'http://%s' % addr

        self.args.nsqd_http_address = addr

        session = requests.Session()
        response = session.get('%s/ping') % addr
        if response.status_code != 200:
            raise Exception("bad response %s" % response)

        url = '%s/ping' % self.args.nsqd_http_address
        self.write_to_nsq = lambda msg: session.post(
                url, data=msg, params={'topic': self.args.nsq_log_topic},
            )

        self.log.info("pushing logs to nsq", url=url, topic=self.args.nsq_log_topic)

    def run(self):
        self.pre_run_init()
        self.log.info('starting process', command=self.args.command)

        # TODO atexit handler to kill this
        self.process = subprocess.Popen(
            self.args.command, bufsize=1, universal_newlines=True,
            stderr=subprocess.PIPE,
        )
        atexit.register(self._on_exit)

        self.log.info("process started", pid=self.process.pid)
        read_stderr_line = self.process.stderr.readline
        poll = self.process.poll

        # TODO bufferring and sending many to nsq together
        while poll() is None:
            line = read_stderr_line()

            try:
                jline = json.loads(line)
                # TODO check that it is really a log
                # TODO send hostname, uuid, timestamp etc.

                # NOTE not using standard log levels because we want this to be transparent
                level = jline.get("level", "debug")
                event = jline.pop("event", "")
                self.log._proxy_to_logger(level, event, **jline)
                self.write_to_nsq(line)

            except ValueError:
                sys.stderr.write(line)

        # read the remaining stderr
        remaining = self.process.stderr.read()
        # TODO this should be processed as well
        sys.stderr.write(remaining)
        sys.stderr.flush()

        sys.exit(self.process.returncode)

    def _on_exit(self):
        # TODO grace period ?
        # TODO make sure all signals go to child process.
        # TODO propagate signals properly ? atexit is being called before signal is sent to child

        poll = self.process.poll()
        if poll is None:
            self.log.warning("killing child process", pid=self.process.pid)
            self.process.terminate()

    def define_args(self, parser):
        super(ProcessWrapper, self).define_args(parser)
        parser.add_argument("--enable-nsq", action="store_true",
            default=False,
            help="push logs to nsq. default: %(default)s",
        )
        parser.add_argument("--nsq-log-topic", default="log",
            help="nsq topic name for logs, default: %(default)s",
        )
        parser.add_argument("--nsqd-http-address",
            default="http://127.0.0.1:4151",
            help="nsqd http address, default: %(default)s",
        )

        parser.add_argument("command", nargs=argparse.REMAINDER,
            help="the command to run",
        )

def main():
    ProcessWrapper().start()

if __name__ == '__main__':
    main()
