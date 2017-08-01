# -*- coding: utf-8 -*-

import multiprocessing
import socket
import signal
import logging
from SocketServer import TCPServer
from HTTPRequestHandler import HTTPRequestHandler
from SimpleHTTPServer import SimpleHTTPRequestHandler


class HttpServer(TCPServer):
    server_name = None
    server_port = None

    def server_bind(self):
        # Changed SO_REUSEADDR to SO_REUSEPORT otherwise we have socket.error: [Errno 98] Address already in use
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.socket.bind(self.server_address)
        self.server_address = self.socket.getsockname()
        host, port = self.socket.getsockname()[:2]
        self.server_name = socket.getfqdn(host)
        self.server_port = port


def run_server(host, port, processes):
    procs = []

    def interrupt_signal(signum, frame):
        for proc in procs:
            if proc:
                name = proc.name
                logging.info('Trying to shutting down {}'.format(name))
                proc.terminate()
                logging.info('{} terminated...'.format(name))

    # Set interrupt signal catcher
    signal.signal(signal.SIGINT, interrupt_signal)

    for i in range(processes):
        server = HttpServer((host, port), SimpleHTTPRequestHandler)
        p = multiprocessing.Process(target=server.serve_forever)
        procs.append(p)
        logging.basicConfig(level=logging.INFO)
        logging.info('Server running on process: {}, host: {}, port: {}'.format(p.name, host, port))
        p.start()

    for proc in procs:
        proc.join()





