#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

import collections
import getopt
import gzip
import glob
import json
import math
import sys
import os
import re

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}

pats = (r''
        r'^\S+\s\S+\s{2}\S+\s\[.*?\]\s'
        r'\"\S+\s(\S+)\s\S+\"\s'  # request_url
        r'\S+\s\S+\s.+?\s\".+?\"\s\S+\s\S+\s\S+\s'
        r'(\S+)'  # request_time
        )

pat = re.compile(pats)


def usage():
    print 'Nginx Log Analyzer Tool'
    print 'The following options are available:'
    print '-l --log_path=/some/path/to/log.gz - path to log file'
    print '-j --json                          - save log analyze as raw json file'
    print '-h --help                          - print this help message and exit'
    sys.exit(0)


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


def round_f(number):
    return round(number, 3)


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
    files = glob.glob(file_dir + '/*')
    if files:
        latest_file = max(files, key=os.path.getmtime)
        return latest_file
    return None


def extract_date_from_file_name(file_path):
    file_date = re.match(r'^.*?(\d{4})(\d{2})(\d{2})\.?\w*$', file_path)
    return '.'.join(file_date.groups())


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


def parse_line(line):
    g = pat.match(line)
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
    print 'Start reading `%s` log file...' % log_path
    log_stat = collections.defaultdict(list)
    total_count = total_time = 0
    report_format = 'html' if is_json is False else 'json'
    report_date = extract_date_from_file_name(log_path)
    report_path = '%s/report-%s.%s' % (config['REPORT_DIR'], report_date, report_format)
    if os.path.isfile(report_path):
        print 'Report `%s` already exists' % report_path
        exit(0)
    try:
        for line in xreadlines(log_path):
            parsed_line = parse_line(line)
            if parsed_line:
                total_count += 1
                total_time += parsed_line['request_time']
                log_stat[parsed_line['request_url']].append(parsed_line['request_time'])
    except IOError as err:
        print str(err)
        sys.exit(1)
    if total_count > 0 and total_time > 0:
        log_report = get_report(log_stat, total_count, total_time, config['REPORT_SIZE'])
        try:
            save_report(log_report, report_path)
        except RuntimeError as err:
            print str(err)
            sys.exit(1)
        print 'Report file `%s` is ready!' % report_path
    else:
        print 'Log `%s` data is empty or has incorrect data' % log_path


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hl:j', ['help', 'log_path=', 'json'])
    except getopt.GetoptError as err:
        print str(err)
        usage()
    log_path = None
    is_json = False
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
        elif o in ("-l", "--log_path"):
            log_path = a
        elif o in ("-j", "--json"):
            is_json = True
        else:
            assert False, 'Unhandled Option'

    if log_path is None:
        log_path = get_latest_file(config['LOG_DIR'])

    if log_path:
        run_analyze(log_path, is_json)
    else:
        print 'No log files into `%s`' % config['LOG_DIR']


if __name__ == "__main__":
    main()
