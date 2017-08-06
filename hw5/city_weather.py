#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import urllib
import urllib2


API_KEY = '702427a7f6ca615420e9301f2cefbc90'


def get_city_by_ip(ip):
    url = 'http://ipinfo.io/{}/json'.format(ip)
    response = urllib2.urlopen(url, timeout=60)
    data = json.loads(response.read())
    return data['city']


def get_weather_by_city(city):
    url = 'http://api.openweathermap.org/data/2.5/weather'
    values = {'q': city,
              'lang': 'ru',
              'units': 'metric',
              'APPID': API_KEY
              }
    encoded_values = urllib.urlencode(values)
    response = urllib2.urlopen(url + '?' + encoded_values)
    return json.loads(response.read())


def main():
    city_ip = '176.14.221.123'
    try:
        city_name = get_city_by_ip(city_ip)
        city_weather = get_weather_by_city(city_name)
        temp = str(city_weather['main']['temp'])
        temp = temp if temp.startswith('-') else '+' + temp
        conditions = city_weather['weather'][0]['description']
        return {'city': city_name, 'temp': temp, 'conditions': conditions}
    except (urllib2.HTTPError, KeyError) as e:
        return {'Error': 'Error `{}` during response'.format(e)}


if __name__ == '__main__':
    print main()
