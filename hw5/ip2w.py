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
    return data.get('city')


def get_weather_by_city(city):
    url = 'http://api.openweathermap.org/data/2.5/weather'
    values = {'q': city,
              'lang': 'ru',
              'units': 'metric',
              'APPID': API_KEY
              }
    return json.loads(do_request(url, values))


def get_weather_by_ip(ip):
    try:
        city_name = get_city_by_ip(ip)
        if not city_name:
            raise RuntimeError('City not found')
        city_weather = get_weather_by_city(city_name)
        try:
            temp = str(city_weather['main']['temp'])
            temp = temp if temp.startswith('-') else '+' + temp
            conditions = city_weather['weather'][0]['description']
        except KeyError:
            raise RuntimeError('Incorrect weather API response')
        response = {'city': city_name, 'temp': temp, 'conditions': conditions}
    except (urllib2.HTTPError, RuntimeError) as e:
        response = {'error': str(e), 'ip': ip}
    return json.dumps(response)


def application(environ, start_response):
    request = environ['PATH_INFO'].split('/')
    ip = request[2]
    content = get_weather_by_ip(ip)
    start_response('200 OK', [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(content)))
    ])
    return [content]
