#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import collections
import argparse
import gzip
import glob
import json
import math
import sys
import os
import re

from datetime import datetime

CONFIG = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}

PATS = (r''
        r'^\S+\s\S+\s{2}\S+\s\[.*?\]\s'
        r'\"\S+\s(\S+)\s\S+\"\s'  # request_url
        r'\S+\s\S+\s.+?\s\".+?\"\s\S+\s\S+\s\S+\s'
        r'(\S+)'  # request_time
        )

PAT = re.compile(PATS)


def parse_args():
    parser = argparse.ArgumentParser(description='Nginx Log Analyzer Tool.')
    parser.add_argument('-l', '--log_path', help='path to log file')
    parser.add_argument('-j', '--json', help='save log analyze as raw json file (default: html)', action='store_true')
    return parser.parse_args()


def get_report(log_stat, total_count, total_time, limit=100):
    report_data = []
    one_count_percent = float(total_count / 100)
    one_time_percent = float(total_time / 100)

    for url, times in log_stat.items():
        count = len(times)
        time_sum = sum(times)
        report_data.append({
            'url': url,
            'time_max': max(times),
            'count': count,
            'time_sum': round_f(time_sum),
            'count_perc': round_f(count / one_count_percent),
            'time_perc': round_f(time_sum / one_time_percent),
            'time_p50': round_f(percentile(times, 50)),
            'time_p95': round_f(percentile(times, 95)),
            'time_p99': round_f(percentile(times, 99))
            })
    report_data.sort(key=lambda x: (x['time_perc'], x['time_sum']), reverse=True)

    return report_data[:limit]


def save_report(report, file_path):
    if file_path.endswith('.html'):
        with open('./report.html', 'r') as f:
            file_data = f.read()
        file_data = file_data.replace('$table_json', json.dumps(report))
        with open(file_path, 'w') as f:
            f.write(file_data)
    elif file_path.endswith('.json'):
        with open(file_path, 'w') as f:
            json.dump(report, f)
    else:
        raise RuntimeError('Unexpected report file format')


def get_latest_file(file_dir):
    files = glob.glob(file_dir + '/nginx-access-ui.log-*')
    if files:
        latest_file = max(files, key=get_file_date)
        return latest_file
    return None


def get_file_date(file_path):
    file_date = re.match(r'^.*-(\d+)\.?\w*$', file_path)
    if file_date:
        return datetime.strptime(file_date.group(1), '%Y%m%d')
    raise RuntimeError('Unexpected log file format')


def percentile(lst, p):
    lst = sorted(lst)
    index = (p / 100.0) * len(lst)
    if math.floor(index) == index:
        result = (lst[int(index)-1] + lst[int(index)]) / 2.0
    else:
        result = lst[int(math.floor(index))]
    return result


def median(lst):
    lst = sorted(lst)
    n = len(lst)
    if n == 0:
        result = 0
    elif n % 2 == 1:
        result = lst[n//2]
    else:
        result = (lst[n//2-1] + lst[n//2]) / 2.0
    return result


def round_f(number):
    return round(number, 3)


def parse_line(line):
    g = PAT.match(line)
    if g:
        col_names = ('request_url', 'request_time')
        parsed_line = (dict(zip(col_names, g.groups())))
        parsed_line['request_time'] = float(parsed_line['request_time']) if parsed_line['request_time'] != '-' else 0
        return parsed_line
    return None


def xreadlines(log_path):
    if log_path.endswith('.gz'):
        log = gzip.open(log_path, 'rb')
    else:
        log = open(log_path)
    for line in log:
        if line:
            yield line
    log.close()


def run_analyze(log_path, is_json):
    report_format = 'json' if is_json else 'html'
    report_date = datetime.strftime(get_file_date(log_path), '%Y.%m.%d')
    report_path = '%s/report-%s.%s' % (CONFIG['REPORT_DIR'], report_date, report_format)
    if os.path.isfile(report_path):
        print 'Report `%s` already exists' % report_path
        exit(0)
    print 'Start reading `%s` log file...' % log_path
    log_stat = collections.defaultdict(list)
    total_count = total_time = 0
    for line in xreadlines(log_path):
        parsed_line = parse_line(line)
        if parsed_line:
            total_count += 1
            total_time += parsed_line['request_time']
            log_stat[parsed_line['request_url']].append(parsed_line['request_time'])
    if total_count > 0 and total_time > 0:
        log_report = get_report(log_stat, total_count, total_time, CONFIG['REPORT_SIZE'])
        save_report(log_report, report_path)
        print 'Report file `%s` is ready!' % report_path
    else:
        print 'Log `%s` data is empty or has incorrect data' % log_path


def main():
    args = parse_args()
    log_path = args.log_path

    if log_path is None:
        log_path = get_latest_file(CONFIG['LOG_DIR'])

    if log_path:
        try:
            run_analyze(log_path, args.json)
        except Exception as err:
            print str(err)
            sys.exit(1)
    else:
        print 'No log files into `%s`' % CONFIG['LOG_DIR']


if __name__ == "__main__":
    main()
