# -*- coding: utf-8 -*-

import multiprocessing
import os
import socket
import logging
import mimetypes
import threading
import urllib

from time import gmtime, strftime
from urlparse import urlparse

BUFFER_SIZE = 1024

OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
NOT_ALLOWED = 405

RESPONSE_CODES = {
    OK: 'OK',
    BAD_REQUEST: 'Bad Request',
    NOT_FOUND: 'Not Found',
    FORBIDDEN: 'Forbidden',
    NOT_ALLOWED: 'Method Not Allowed',
}

ALLOWED_CONTENT_TYPES = (
    'text/css',
    'text/html',
    'application/javascript',
    'image/jpeg',
    'image/png',
    'image/gif',
    'text/plain',
    'application/x-shockwave-flash'
)

ALLOWED_METHODS = ('GET', 'HEAD')

SERVER_VERSION = 'OtusServer'

PROTOCOL_VERSION = 'HTTP/1.1'

HTTP_HEAD_TERMINATOR = '\r\n\r\n'


class HTTPRequestHandler(object):
    document_root = 'http'
    index_file = 'index.html'

    def __init__(self, connection, client_address):
        self.method = None
        self.path = None
        self.body = ''
        self.is_directory = False
        self.response_headers = {}
        self.request_headers = {}
        self.connection = connection
        self.client_address = client_address
        self.close_connection = 1
        self.process = multiprocessing.current_process().name
        self.thread = threading.current_thread().name
        try:
            self.run()
        finally:
            self.finish()

    def do_GET(self):
        logging.debug('Get file | P: %s | T: %s | Path: %s', self.process, self.thread, self.path)
        status = self.set_head()
        if status == OK:
            return self.send_response(self.set_body())
        return self.send_response(status)

    def do_HEAD(self):
        return self.send_response(self.set_head())

    def run(self):
        self.handle_request()
        while self.close_connection == 0:
            self.handle_request()

    def finish(self):
        logging.debug('Socket close | P: %s | T: %s', self.process, self.thread)
        self.connection.close()

    def handle_request(self):
        raw_request_line = self.recvall()
        response_code = self.parse_request(raw_request_line)
        if response_code != OK:
            self.send_response(response_code)
            return None
        method = getattr(self, 'do_' + self.method)
        return method()

    def parse_request(self, request_line):
        request_lines = request_line.split('\r\n')
        first_line = request_lines[0].split()
        if len(first_line) != 3:
            return BAD_REQUEST
        method, url, version = first_line
        if method not in ALLOWED_METHODS:
            return NOT_ALLOWED
        del request_lines[0]
        for line in request_lines:
            if not line.split():
                break
            k, v = line.split(':', 1)
            self.request_headers[k.lower()] = v.strip()
        logging.debug('Request url | P: %s | T: %s | Url: %s', self.process, self.thread, url)
        parsed_url = urlparse(url)
        parsed_path = urllib.unquote(parsed_url.path).decode('utf8')
        self.is_directory = parsed_path.endswith('/')
        self.path = self.document_root + os.path.realpath(parsed_path)
        self.method = method.upper()
        return OK

    def set_header(self, keyword, value):
        self.response_headers[keyword] = value
        if keyword.lower() == 'connection':
            if value.lower() == 'close':
                self.close_connection = 1
            elif value.lower() == 'keep-alive':
                self.close_connection = 0

    def set_head(self):
        need_index = False
        if self.is_directory:
            self.path = os.path.join(self.path, self.index_file)
            need_index = True
        if os.path.isfile(self.path):
            ctype, _ = mimetypes.guess_type(self.path)
            if ctype not in ALLOWED_CONTENT_TYPES:
                return NOT_ALLOWED
            try:
                fs = os.path.getsize(self.path)
            except os.error:
                return NOT_ALLOWED
            self.set_header('Content-Type', ctype)
            self.set_header('Content-Length', str(fs))
            if 'connection' in self.request_headers:
                self.set_header('Connection', self.request_headers['connection'])
            return OK
        else:
            return FORBIDDEN if need_index else NOT_FOUND

    def set_body(self):
        try:
            logging.debug('Load file | P: %s | T: %s | Path: %s', self.process, self.thread, self.path)
            with open(self.path, 'r') as f:
                self.body = f.read(int(self.response_headers['Content-Length']))
        except IOError:
            logging.debug('Failed file | P: %s | T: %s | Path: %s', self.process, self.thread, self.path)
            return NOT_ALLOWED
        return OK

    def send_response(self, code):
        logging.debug('Response | P: %s | T: %s | Code: %d | Path: %s', self.process, self.thread, code, self.path)
        first_line = '%s %d %s' % (PROTOCOL_VERSION, code, RESPONSE_CODES[code])
        self.set_header('Server', SERVER_VERSION)
        self.set_header('Date', self.date_time_string())
        if code != OK:
            self.set_header('Content-Type', 'text/html')
            self.set_header('Content-Length', '0')
            self.set_header('Connection', 'close')
        headers = '\r\n'.join('%s: %s' % (k, v) for k, v in self.response_headers.items())
        try:
            self.connection.sendall(
                '%s\r\n%s%s%s' % (first_line, headers, HTTP_HEAD_TERMINATOR, self.body if code == OK else '')
            )
        except socket.error:
            logging.debug('Sendall socket error | P: %s | T: %s', self.process, self.thread)

    def recvall(self):
        raw_request_line = ''
        while True:
            data = self.connection.recv(BUFFER_SIZE)
            raw_request_line += data
            if raw_request_line.find(HTTP_HEAD_TERMINATOR) >= 0 or not data:  # Doesn't read body of request
                break
        return raw_request_line

    @staticmethod
    def date_time_string():
        return strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime())


class HTTPThreadingServer(object):
    request_queue_size = 1024

    def __init__(self, host, port, request_handler):
        self.sock = None
        self.host = host
        self.port = port
        self.request_handler = request_handler
        self.create_socket()

    def create_socket(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self.sock.bind((self.host, self.port))
            self.sock.listen(self.request_queue_size)
        except socket.error as e:
            raise RuntimeError(e)

    def serve_forever(self):
        while True:
            try:
                conn, addr = self.sock.accept()
                logging.debug('Connected | P: %s | PID: %d', multiprocessing.current_process().name, os.getpid())
                t = threading.Thread(target=self.request_handler, args=(conn, addr))
                t.daemon = True
                t.start()
                logging.debug('Request handler running | P: %s | T: %s', multiprocessing.current_process().name, t.name)
            except socket.error:
                self.sock.close()
