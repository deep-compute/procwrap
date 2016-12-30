#!/usr/bin/env python

import os
import sys
import time
import json
import Queue
import atexit
import signal
import requests
import argparse
import datetime
import threading
import subprocess
from uuid import uuid1
from urlparse import urljoin
from basescript import BaseScript

class ProcessWrapper(BaseScript):
    DESC = "process wrapper"

    def pre_run_init(self):
        self.write_to_nsq = lambda msg: None
        self.process = None

        if not self.args.enable_nsq:
            return

        self.init_nsq()

    def init_nsq(self):
        """
        if --enable-nsq is provided, initialize the nsq
        """

        if not self.args.nsqd_http_address.startswith('http'):
            self.args.nsqd_http_address = 'http://%s' % self.args.nsqd_http_address


        url = urljoin(self.args.nsqd_http_address, '/ping')
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("bad response %s" % response)

        # TODO make queuesize configurable
        queue = Queue.Queue(maxsize=1000)
        # block until you can write it to the queue
        self.write_to_nsq = lambda msg: queue.put(msg, block=True, timeout=None)

        self.queue = queue
        self.nsq_publish_thread = threading.Thread(target=self._publish_to_nsq)
        self.nsq_publish_thread.daemon = True
        self.keeprunning = threading.Event()
        self.keeprunning.set()

        self.nsq_publish_thread.start()

    def _publish_to_nsq(self):
        url = urljoin(self.args.nsqd_http_address, '/mpub')
        topic = self.args.nsq_log_topic
        self.log.info("pushing logs to nsq", url=url, topic=topic)

        max_content_length = self.args.nsq_max_content_length
        interval = datetime.timedelta(milliseconds=self.args.nsq_log_interval)
        queue_get_timeout = interval.total_seconds()

        buf = []
        size = 0
        last_send = datetime.datetime.now() - (100 * interval)

        session = requests.Session()
        params = { 'topic': topic }

        keeprunning = self.keeprunning
        queue = self.queue

        keeprunning.wait()
        while keeprunning.is_set():
            now = datetime.datetime.now()
            try:
                msg = queue.get(block=True, timeout=queue_get_timeout)
                size += len(msg)
                buf.append(msg)
            except Queue.Empty:
                pass

            if size < max_content_length and (now - last_send) < interval:
                continue

            if len(buf) == 0:
                continue

            # NOTE buf cannot have \n in it. json escapes \n so thats fine.
            try:
                self.log.debug("sending logs to nsq", num_logs=len(buf))

                data = '\n'.join(buf)
                resp = session.post(url, data=data, params=params)
                if resp.status_code != 200:
                    raise Exception("bad response %s sending to nsq" % resp)

                last_send = now
                buf = []
                size = 0

            except:
                self.log.exception("failed to send to nsq")

        # sending whatever is remaining
        if len(buf) != 0:
            try:
                self.log.debug("sending remaining logs to nsq", num_logs=len(buf))
                data = '\n'.join(buf)
                resp = session.post(url, data=data, params=params)
                if resp.status_code != 200:
                    raise Exception("bad response %s sending to nsq" % resp)

            except:
                self.log.exception("failed to send to nsq...")

        self.log.warning("stopped sending logs to nsq")

    def run(self):
        self.pre_run_init()
        self.log.info('starting process', command=self.args.command)

        def become_tty_fg():
            # make child process use the fg
            # http://stackoverflow.com/questions/15200700/how-do-i-set-the-terminal-foreground-process-group-for-a-process-im-running-und
            os.setpgrp()
            hdlr = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
            tty = os.open('/dev/tty', os.O_RDWR)
            os.tcsetpgrp(tty, os.getpgrp())
            signal.signal(signal.SIGTTOU, hdlr)

        # TODO atexit handler to kill this
        self.process = subprocess.Popen(
            self.args.command, bufsize=1, universal_newlines=True,
            stderr=subprocess.PIPE,
            preexec_fn=become_tty_fg,
        )

        self.log.info("process started", pid=self.process.pid)
        read_stderr_line = self.process.stderr.readline
        poll = self.process.poll

        hostname = self.hostname # from basescript
        while poll() is None:
            line = read_stderr_line()

            try:
                jline = json.loads(line)

                level = jline.get("level", "debug")
                event = jline.pop("event", "")
                # NOTE not using standard log levels because we want this to be transparent
                self.log._proxy_to_logger(level, event, **jline)

                jline['event'] = event
                jline['uuid'] = str(uuid1())
                jline['host'] = hostname
                # we can get the timestamp from the uuid itself, so no need for timestamp
                self.write_to_nsq(json.dumps(jline))

            except ValueError:
                sys.stderr.write(line)

        # read the remaining stderr
        remaining = self.process.stderr.read()
        # TODO this should be processed as well
        sys.stderr.write(remaining)
        sys.stderr.flush()

        sys.exit(self.process.returncode)

    def on_exit(self):
        # TODO grace period ?
        # TODO make sure all signals go to child process.
        # TODO propagate signals properly ? atexit is being called before signal is sent to child

        if self.process is not None:
            poll = self.process.poll()
            if poll is None:
                self.log.warning("killing child process", pid=self.process.pid)
                self.process.terminate()

        if self.args.enable_nsq:
            # nsq was enabled
            self.log.info("waiting to publish remaining logs to nsq...")
            self.keeprunning.clear()
            self.nsq_publish_thread.join()

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
        parser.add_argument("--nsq-log-interval",
            type=int,
            default=500,
            help=(
                "number of milliseconds to buffer before pushing logs, "
                "default: %(default)s"),
        )
        parser.add_argument("--nsq-max-content-length",
            type=int,
            default=1024 * 1024,
            help=(
                "number of bytes to buffer before pushing logs to nsq, "
                "default: %(default)s"),
        )
        parser.add_argument("command", nargs=argparse.REMAINDER,
            help="the command to run",
        )

def main():
    pw = ProcessWrapper()
    atexit.register(pw.on_exit)
    pw.start()

if __name__ == '__main__':
    main()
