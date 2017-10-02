#!/usr/bin/env python
# -*- coding: utf-8 -*-
import Queue
import os
import gzip
import sys
import glob
import logging
import collections
import threading
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


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
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info('Not all user apps are digits: `%s`', line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info('Invalid geo coords: `%s`', line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_appsinstalled(queue, device_memc):
    processed = errors = 0

    while True:
        try:
            task = queue.get(timeout=0.1)
        except Queue.Empty:
            logging.info('%s | Records inserted: %s', threading.current_thread().name, processed)
            if processed:
                err_rate = float(errors) / processed
                if err_rate < NORMAL_ERR_RATE:
                    logging.info('Acceptable error rate (%s). Successfull load', err_rate)
                else:
                    logging.error('High error rate (%s > %s). Failed load', err_rate, NORMAL_ERR_RATE)
            return

        pools, line, dry_run = task
        appsinstalled = parse_appsinstalled(line)

        if not appsinstalled:
            errors += 1
            continue

        memc_addr = device_memc.get(appsinstalled.dev_type)

        if not memc_addr:
            errors += 1
            logging.error('Unknow device type: %s', appsinstalled.dev_type)
            continue

        memc_pool = pools[memc_addr]

        try:
            memc = memc_pool.get(timeout=0.01)
        except Queue.Empty:
            memc = memcache.Client([memc_addr])

        key, ua = buf_appsinstalled(appsinstalled)

        try:
            if dry_run:
                logging.debug('%s - %s -> %s', memc_addr, key, str(ua).replace('\n', ' '))
                status = 1
            else:
                status = memc.set(key, ua.SerializeToString())
            if status != 0:
                processed += 1
            else:
                errors += 1
        except Exception as e:
            logging.exception('Cannot write to memc %s: %s', memc_addr, e)

        memc_pool.put(memc)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    pools = collections.defaultdict(Queue.Queue)
    queue = Queue.Queue()
    workers = []

    for i in range(int(options.threads)):
        thread = threading.Thread(target=insert_appsinstalled, args=(queue, device_memc,))
        thread.daemon = True
        workers.append(thread)

    for thread in workers:
        thread.start()

    for fn in glob.iglob(options.pattern):
        logging.info('Processing %s', fn)
        lines_counter = 0
        fd = gzip.open(fn)
        for line in fd:
            line = line.strip()
            if not line:
                continue
            lines_counter += 1
            queue.put((pools, line, options.dry))
        logging.info('File %s | Total records inserted: %s', fn, lines_counter)
        fd.close()
        dot_rename(fn)

    for thread in workers:
        thread.join()


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
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
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--threads", action="store", default="4")
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
