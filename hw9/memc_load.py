#!/usr/bin/env python
# -*- coding: utf-8 -*-
import Queue
import os
import gzip
import sys
import glob
import time
import logging
import collections
import threading
import multiprocessing
from itertools import islice
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

MEMCACHE_SOCKET_TIMEOUT = 2
NORMAL_ERR_RATE = 0.01
CHUNK_SIZE = 100
SENTINEL = object()

AppsInstalled = collections.namedtuple('AppsInstalled', ['dev_type', 'dev_id', 'lat', 'lon', 'apps'])


class MemcachedThread(threading.Thread):
    def __init__(self, memc_addr, queue, stats_queue, dry_run):
        threading.Thread.__init__(self)
        self.daemon = True
        self.memc_addr = memc_addr
        self.queue = queue
        self.stats_queue = stats_queue
        self.dry_run = dry_run
        self.processed = 0
        self.errors = 0

    @staticmethod
    def _set_multi(connection, keys_map, tries=3, delay=0.5, backoff=2):
        failed_keys = connection.set_multi(keys_map)
        mtries, mdelay = tries, delay
        while failed_keys and mtries > 0:
            time.sleep(mdelay)
            keys_map = {k: v for k, v in keys_map.items() if k in failed_keys}
            failed_keys = connection.set_multi(keys_map)
            mtries -= 1
            mdelay *= backoff
        return failed_keys

    def run(self):
        connection = memcache.Client([self.memc_addr], socket_timeout=MEMCACHE_SOCKET_TIMEOUT)
        while True:
            try:
                tasks = self.queue.get(timeout=0.1)

                if tasks == SENTINEL:
                    self.stats_queue.put((self.processed, self.errors))
                    logging.info('%s | %s | Records processed: %d | Records errors: %d',
                                 multiprocessing.current_process().name,
                                 threading.current_thread().name, self.processed, self.errors)
                    self.queue.task_done()
                    break

                try:
                    if self.dry_run:
                        for task in tasks:
                            key, ua = task
                            logging.debug('%s - %s -> %s', self.memc_addr, key, str(ua).replace('\n', ' '))
                    else:
                        keys_map = {k: v.SerializeToString() for k, v in tasks}
                        failed_keys = self._set_multi(connection, keys_map)
                        self.processed += len(tasks) - len(failed_keys)
                        self.errors += len(failed_keys)
                except Exception as e:
                    logging.exception('Cannot write to memc %s: %s', self.memc_addr, e)

                self.queue.task_done()
            except Queue.Empty:
                continue


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, '.' + fn))


def buf_appsinstalled(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = '{}:{}'.format(appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    return key, ua


def parse_appsinstalled(line):
    line_parts = line.strip().split('\t')
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(',')]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(',') if a.isidigit()]
        logging.info('Not all user apps are digits: `%s`', line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info('Invalid geo coords: `%s`', line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def lines_chunk_handler(chunk, conn_pools):
    errors = 0

    packed_chunks = collections.defaultdict(list)

    for line in chunk:
        line = line.strip()

        if not line:
            continue

        appsinstalled = parse_appsinstalled(line)

        if not appsinstalled:
            errors += 1
            continue

        key, ua = buf_appsinstalled(appsinstalled)

        packed_chunks[appsinstalled.dev_type].append((key, ua))

    for k, v in packed_chunks.items():
        conn_queue = conn_pools.get(k)
        if not conn_queue:
            errors += 1
            logging.error('Unknow device type: %s', k)
            continue
        conn_queue.put(v)

    return errors


def handle_log((fn, device_memc, dry_run)):
    stats_queue = Queue.Queue()
    connections = []
    conn_pools = collections.defaultdict(Queue.Queue)

    # Start memcached connection handle threads
    for k, addr in device_memc.items():
        memcached_thread = MemcachedThread(addr, conn_pools[k], stats_queue, dry_run)
        connections.append(memcached_thread)

    for memcached_thread in connections:
        memcached_thread.start()

    logging.info('%s | Processing %s', multiprocessing.current_process().name, fn)

    processed = read = errors = 0
    with gzip.open(fn) as fd:
        chunk = list(islice(fd, CHUNK_SIZE))
        while all(c.is_alive() for c in connections):
            if not chunk:
                break
            errors += lines_chunk_handler(chunk, conn_pools)
            read += len(chunk)
            chunk = list(islice(fd, CHUNK_SIZE))

    logging.info('%s | File: %s | Total records readed: %s', multiprocessing.current_process().name, fn,
                 read)

    # Finish memcached connection handle threads
    for k, addr in device_memc.items():
        conn_pools[k].put(SENTINEL)

    for memcached_thread in connections:
        memcached_thread.join()

    while not stats_queue.empty():
        worker_processed, worker_errors = stats_queue.get(timeout=0.1)
        processed += worker_processed
        errors += worker_errors

    logging.info('%s | File: %s | Total records processed: %d | Total records errors: %d',
                 multiprocessing.current_process().name, fn, processed, errors)

    if processed:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info('%s | File: %s | Acceptable error rate (%s). Successfull load',
                         multiprocessing.current_process().name, fn, err_rate)
        else:
            logging.error('%s | File: %s | High error rate (%s > %s). Failed load',
                          multiprocessing.current_process().name, fn, err_rate,
                          NORMAL_ERR_RATE)

    return fn


def main(options):
    device_memc = {
        'idfa': options.idfa,
        'gaid': options.gaid,
        'adid': options.adid,
        'dvid': options.dvid,
    }

    pool = multiprocessing.Pool(int(options.workers))
    fargs = []

    for fn in glob.iglob(options.pattern):
        fargs.append((fn, device_memc, options.dry))

    fargs.sort(key=lambda x: x[0])

    for f in pool.imap(handle_log, fargs):
        dot_rename(f)
        logging.info('%s | Renamed file %s', multiprocessing.current_process().name, f)


def prototest():
    sample = 'idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424'
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split('\t')
        apps = [int(a) for a in raw_apps.split(',') if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option('-t', '--test', action='store_true', default=False)
    op.add_option('-l', '--log', action='store', default=None)
    op.add_option('--dry', action='store_true', default=False)
    op.add_option('--pattern', action='store', default='/data/appsinstalled/*.tsv.gz')
    op.add_option('--idfa', action='store', default='127.0.0.1:33013')
    op.add_option('--gaid', action='store', default='127.0.0.1:33014')
    op.add_option('--adid', action='store', default='127.0.0.1:33015')
    op.add_option('--dvid', action='store', default='127.0.0.1:33016')
    op.add_option('--workers', action='store', default=2)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info('Memc loader started with options: %s', opts)

    try:
        main(opts)
    except Exception as e:
        logging.exception('Unexpected error: %s', e)
        sys.exit(1)
