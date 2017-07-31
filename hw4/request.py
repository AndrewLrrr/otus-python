# -*- coding: utf-8 -*-

import multiprocessing
from SocketServer import BaseRequestHandler


class TCPRequestHandler(BaseRequestHandler):
    """ An example of threaded TCP request handler """
    def handle(self):
        data = self.request.recv(1024)
        current_thread = multiprocessing.current_process()
        response = "%s: %s" % (current_thread.name, data)
        self.request.sendall(response)
