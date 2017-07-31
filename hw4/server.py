# -*- coding: utf-8 -*-

import multiprocessing
import socket
import signal
import logging
from request import TCPRequestHandler
from SocketServer import TCPServer


class CustomTCPServer(TCPServer):
    def server_bind(self):
        # Changed SO_REUSEADDR to SO_REUSEPORT otherwise we have socket.error: [Errno 98] Address already in use
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.socket.bind(self.server_address)
        self.server_address = self.socket.getsockname()


def run_server(host, port, processes):
    procs = []

    def interrupt_signal(signum, frame):
        for proc in procs:
            if proc:
                name = proc.name
                print 'Trying to shutting down {}'.format(name)
                proc.terminate()
                print '{} terminated...'.format(name)

    # Set interrupt signal catcher
    signal.signal(signal.SIGINT, interrupt_signal)

    for i in range(processes):
        server = CustomTCPServer((host, port), TCPRequestHandler)
        p = multiprocessing.Process(target=server.serve_forever)
        procs.append(p)
        logging.warning('Server running on process: {}, host: {}, port: {}'.format(p.name, host, port))
        p.start()

    for proc in procs:
        proc.join()





