#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import urllib
import urllib2
import logging

from functools import wraps

logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s', level=logging.WARNING,
                    datefmt='%a %b %d %H:%M:%S %Y')


def retry(ExceptionToCheck, tries=3, delay=3, backoff=2):
    """Retry calling the decorated function using an exponential backoff.
    https://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    logging.warning('%s, Retrying in %d seconds...', str(e), mdelay)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


@retry(urllib2.URLError)
def do_request(url, data=None):
    encoded_data = '?' + urllib.urlencode(data) if data else ''
    response = urllib2.urlopen(url + encoded_data, timeout=30)
    return response.read()


def get_geo_data_by_ip(ip):
    url = 'http://ipinfo.io/{}/json'.format(ip)
    return json.loads(do_request(url))


def get_weather(lat, lon):
    try:
        api_key = os.environ['OPEN_WEATHER_API_KEY']
    except KeyError:
        raise EnvironmentError('Set OPEN_WEATHER_API_KEY environment key')
    url = 'http://api.openweathermap.org/data/2.5/weather'
    values = {'lat': lat,
              'lon': lon,
              'lang': 'ru',
              'units': 'metric',
              'APPID': api_key
              }
    return json.loads(do_request(url, values))


def get_weather_by_ip(ip):
    try:
        geo_data = get_geo_data_by_ip(ip)
        city_weather = get_weather(*geo_data.get('loc').split(','))
        city = city_weather['name']
        temp = str(city_weather['main']['temp'])
        temp = temp if temp.startswith('-') else '+' + temp
        conditions = city_weather['weather'][0]['description']
        return '200', {'city': city, 'temp': temp, 'conditions': conditions}
    except urllib2.HTTPError as e:
        return str(e.getcode()), {'error': str(e)}
    except urllib2.URLError as e:
        logging.error('URLError: %s', repr(e))
        return '504', {'error': 'Gateway Timeout'}
    except Exception as e:
        logging.error('Exception: %s', repr(e))
        return '500', {'error': 'Internal error'}


def application(environ, start_response):
    request = environ['PATH_INFO'].strip('/').split('/')
    try:
        ip = request[1]
    except IndexError:
        ip = environ['REMOTE_ADDR']
    code, respond = get_weather_by_ip(ip)
    respond = json.dumps(respond)
    start_response(code, [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(respond)))
    ])
    return [respond]
