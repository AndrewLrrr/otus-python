#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import urllib
import urllib2


API_KEY = '702427a7f6ca615420e9301f2cefbc90'


def do_request(url, data=None):
    encoded_data = '?' + urllib.urlencode(data) if data else ''
    response = urllib2.urlopen(url + encoded_data, timeout=60)
    return response.read()


def get_city_by_ip(ip):
    url = 'http://ipinfo.io/{}/json'.format(ip)
    data = json.loads(do_request(url))
    return data['city']


def get_weather_by_city(city):
    url = 'http://api.openweathermap.org/data/2.5/weather'
    values = {'q': city,
              'lang': 'ru',
              'units': 'metric',
              'APPID': API_KEY
              }
    return json.loads(do_request(url, values))


def get_weather_by_city_ip(ip):
    try:
        city_name = get_city_by_ip(ip)
        city_weather = get_weather_by_city(city_name)
        temp = str(city_weather['main']['temp'])
        temp = temp if temp.startswith('-') else '+' + temp
        conditions = city_weather['weather'][0]['description']
        response = {'city': city_name, 'temp': temp, 'conditions': conditions}
    except (urllib2.HTTPError, KeyError) as e:
        response = {'error': str(e), 'ip': ip}
    return json.dumps(response)


def application(environ, start_response):
    ip = '176.14.221.123'
    content = get_weather_by_city_ip(ip)
    start_response('200 OK', [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(content)))
    ])
    return [content]
