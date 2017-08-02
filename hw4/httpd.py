#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import multiprocessing
import signal

import logging

from server import SimpleHTTPRequestHandler, SimpleHTTPServer


def run_server(host, port, workers, debug):
    processes = []

    def interrupt_signal(signum, frame):
        for process in processes:
            if process:
                name = process.name
                print 'Trying to shutting down %s' % name
                process.terminate()
                print '%s terminated...' % name

    # Set interrupt signal catcher
    signal.signal(signal.SIGINT, interrupt_signal)

    for i in range(workers):
        server = SimpleHTTPServer(host, port, SimpleHTTPRequestHandler)
        p = multiprocessing.Process(target=server.serve_forever)
        processes.append(p)
        logging.basicConfig(level=(logging.DEBUG if debug else logging.ERROR))
        print 'Server running on the process: %s, host: %s, port: %d' % (p.name, host, port)
        p.start()

    for proc in processes:
        proc.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-hs', '--host', default='127.0.0.1')
    parser.add_argument('-p', '--port', default=8080)
    parser.add_argument('-w', '--workers', default=2)
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    run_server(args.host, int(args.port), int(args.workers), args.debug)
