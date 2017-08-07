#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import urllib
import urllib2


def do_request(url, data=None):
    encoded_data = '?' + urllib.urlencode(data) if data else ''
    response = urllib2.urlopen(url + encoded_data, timeout=60)
    return response.read()


def get_geo_data_by_ip(ip):
    url = 'http://ipinfo.io/{}/json'.format(ip)
    return json.loads(do_request(url))


def get_weather(lat, lon):
    api_key = os.environ['OPEN_WEATHER_API_KEY']
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
        return {
            'code': '200',
            'message': json.dumps({'city': city, 'temp': temp, 'conditions': conditions})
        }
    except urllib2.HTTPError as e:
        return {'code': str(e.getcode()), 'message': json.dumps({'error': str(e)})}


def application(environ, start_response):
    request = environ['PATH_INFO'].split('/')
    ip = request[2]
    respond = get_weather_by_ip(ip)
    start_response(respond['code'], [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(respond['message'])))
    ])
    return [respond['message']]
