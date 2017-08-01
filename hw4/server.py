# -*- coding: utf-8 -*-

import multiprocessing
import os
import socket
import signal
import logging
import mimetypes

from time import gmtime, strftime
from urlparse import urlparse

BUFFER_SIZE = 1024

RESPONSE_CODES = {
    200: 'OK',
    400: 'Bad Request',
    404: 'Not Found',
    405: 'Method Not Allowed',
}

ALLOWED_CONTENT_TYPES = [
    'text/css',
    'text/html',
    'application/javascript',
    'image/jpeg',
    'image/png',
    'image/gif',
    'text/plain',
    'application/x-shockwave-flash'
]

ALLOWED_METHODS = ['GET', 'HEAD']

SERVER_VERSION = 'OtusServer'

PROTOCOL_VERSION = 'HTTP/1.1'


class SimpleHttpRequestHandler(object):
    index_file = 'index.html'
    document_root = 'httptest'

    def __init__(self, connection, client_address):
        self.method = None
        self.path = None
        self.body = ''
        self.response_headers = {}
        self.request_headers = {}
        self.connection = connection
        self.client_address = client_address
        self.close_connection = 1
        try:
            self.run()
        finally:
            self.finish()

    def do_GET(self):
        if self.set_head() and self.set_body():
            self.send_response(200)

    def do_HEAD(self):
        if self.set_head():
            self.send_response(200)

    def run(self):
        self.handle_request()
        while not self.close_connection:
            self.handle_request()

    def finish(self):
        self.connection.close()

    def handle_request(self):
        raw_request_line = self.connection.recv(BUFFER_SIZE)
        if not raw_request_line:
            self.close_connection = 1
            return None
        if not self.parse_request(raw_request_line):
            return None
        method = getattr(self, 'do_' + self.method)
        method()

    def parse_request(self, request_line):
        request_lines = request_line.split('\r\n')
        first_line = request_lines[0].split()
        if len(first_line) != 3:
            self.send_error(400)
            return False
        method, path, version = request_lines[0].split()
        if method not in ALLOWED_METHODS:
            self.send_error(405)
            return False
        del request_lines[0]
        for line in request_lines:
            if line.split():
                k, v = line.split(':', 1)
                self.request_headers[k.lower()] = v.strip()
        parsed = urlparse(path)
        self.path = self.document_root + parsed.path
        self.method = method
        return True

    def set_header(self, keyword, value):
        self.response_headers[keyword] = value
        if keyword.lower() == 'connection':
            if value.lower() == 'close':
                self.close_connection = 1
            elif value.lower() == 'keep-alive':
                self.close_connection = 0

    def set_head(self):
        if self.path.endswith('/'):
            self.path = os.path.join(self.path, self.index_file)
        if os.path.isfile(self.path):
            ctype, _ = mimetypes.guess_type(self.path)
            if ctype not in ALLOWED_CONTENT_TYPES:
                self.send_error(405)
                return False
            try:
                fs = os.path.getsize(self.path)
            except os.error:
                self.send_error(404)
                return False
            self.set_header('Content-type', ctype)
            self.set_header('Content-Length', str(fs))
            if 'connection' in self.request_headers:
                self.set_header('Connection', self.request_headers['connection'])
            return True
        else:
            self.send_error(404)
            return False

    def set_body(self):
        try:
            with open(self.path, 'r') as f:
                self.body = f.read()
        except IOError:
            self.send_error(404)
            return False
        return True

    def send_error(self, code):
        self.set_header('Content-Type', 'text/html')
        self.set_header('Connection', 'close')
        self.send_response(code)

    def send_response(self, code):
        first_line = '%s %d %s' % (PROTOCOL_VERSION, code, RESPONSE_CODES[code])
        self.set_header('Server', SERVER_VERSION)
        self.set_header('Date', self.date_time_string())
        headers = '\r\n'.join('%s: %s' % (k, v) for k, v in self.response_headers.items())
        self.connection.sendall('%s\r\n%s\r\n\r\n%s' % (first_line, headers, self.body))

    @staticmethod
    def date_time_string():
        return strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime())


class SimpleHTTPServer(object):
    request_queue_size = 5

    def __init__(self, port, host, request_handler, timeout=30, sock=None):
        self.port = port
        self.host = host
        self.request_handler = request_handler
        self.timeout = timeout
        self.sock = sock
        if self.sock is None:
            self.create_socket()

    def create_socket(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            if self.timeout:
                s.settimeout(self.timeout)
            s.bind((self.port, self.host))
            s.listen(self.request_queue_size)
        except socket.error as e:
            raise RuntimeError(e)
        self.sock = s

    def serve_forever(self):
        while True:
            try:
                conn, addr = self.sock.accept()
                logging.debug('Connected %s to %d' % (multiprocessing.current_process().name, os.getpid()))
                self.request_handler(conn, addr)
            except socket.timeout:
                self.sock.close()
                logging.debug('Timeout %s to %d' % (multiprocessing.current_process().name, os.getpid()))
                self.create_socket()


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
        server = SimpleHTTPServer(host, port, SimpleHttpRequestHandler)
        p = multiprocessing.Process(target=server.serve_forever)
        processes.append(p)
        logging.basicConfig(level=(logging.DEBUG if debug else logging.ERROR))
        print 'Server running into the process: %s, host: %s, port: %d' % (p.name, host, port)
        p.start()

    for proc in processes:
        proc.join()
