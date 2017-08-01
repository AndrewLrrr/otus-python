# -*- coding: utf-8 -*-
import argparse
import socket

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8080
BUF_SIZE = 1024


def client(ip, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    try:
        sock.sendall(message)
        response = sock.recv(BUF_SIZE)
        print "Client received: %s" % response
    finally:
        sock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', default=4)
    args = parser.parse_args()

    for c in range(int(args.count)):
        client(SERVER_HOST, SERVER_PORT, 'Hello from client {}'.format(c))
