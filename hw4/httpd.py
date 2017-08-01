#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import server


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-hs', '--host', default='0.0.0.0')
    parser.add_argument('-p', '--port', default=8080)
    parser.add_argument('-w', '--workers', default=2)
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    server.run_server(args.host, int(args.port), int(args.workers), args.debug)
