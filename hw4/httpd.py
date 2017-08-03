#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import multiprocessing
import logging

from server import HTTPRequestHandler, HTTPThreadingServer


def run_server(host, port, workers, debug):
    processes = []

    try:
        for i in range(workers):
            server = HTTPThreadingServer(host, port, HTTPRequestHandler)
            p = multiprocessing.Process(target=server.serve_forever)
            processes.append(p)
            logging.basicConfig(level=(logging.DEBUG if debug else logging.ERROR))
            p.start()
            print 'Server running on the process: %d, host: %s, port: %d' % (p.pid, host, port)
        for proc in processes:
            proc.join()
    except KeyboardInterrupt:
        for process in processes:
            if process:
                pid = process.pid
                print 'Trying to shutting down process %s' % pid
                process.terminate()
                print 'Process %s terminated...' % pid


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Otus Web Server')
    parser.add_argument('-s', '--host', default='127.0.0.1', help='Host')
    parser.add_argument('-p', '--port', default=8080, help='Port')
    parser.add_argument('-w', '--workers', default=2, help='Count of workers')
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages')
    args = parser.parse_args()

    run_server(args.host, int(args.port), int(args.workers), args.debug)
