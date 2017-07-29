#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Нужно реализовать простое HTTP API сервиса скоринга. Шаблон уже есть в api.py, тесты в test.py.
# API необычно тем, что польщователи дергают методы POST запросами. Чтобы получить результат
# пользователь отправляет в POST запросе валидный JSON определенного формата на локейшн /method

# Структура json-запроса:

# {"account": "<имя компании партнера>", "login": "<имя пользователя>", "method": "<имя метода>",
#  "token": "<аутентификационный токен>", "arguments": {<словарь с аргументами вызываемого метода>}}

# account - строка, опционально, может быть пустым
# login - строка, обязательно, может быть пустым
# method - строка, обязательно, может быть пустым
# token - строка, обязательно, может быть пустым
# arguments - словарь (объект в терминах json), обязательно, может быть пустым

# Валидация:
# запрос валиден, если валидны все поля по отдельности

# Структура ответа:
# {"code": <числовой код>, "response": {<ответ вызываемого метода>}}
# {"code": <числовой код>, "error": {<сообщение об ошибке>}}

# Аутентификация:
# смотри check_auth в шаблоне. В случае если не пройдена, нужно возвращать
# {"code": 403, "error": "Forbidden"}

# Метод online_score.
# Аргументы:
# phone - строка или число, длиной 11, начинается с 7, опционально, может быть пустым
# email - строка, в которой есть @, опционально, может быть пустым
# first_name - строка, опционально, может быть пустым
# last_name - строка, опционально, может быть пустым
# birthday - дата в формате DD.MM.YYYY, с которой прошло не больше 70 лет, опционально, может быть пустым
# gender - число 0, 1 или 2, опционально, может быть пустым

# Валидация аругементов:
# аргументы валидны, если валидны все поля по отдельности и если присутсвует хоть одна пара
# phone-email, first name-last name, gender-birthday с непустыми значениями.

# Контекст
# в словарь контекста должна прописываться запись  "has" - список полей,
# которые были не пустые для данного запроса

# Ответ:
# в ответ выдается произвольное число, которое больше или равно 0
# {"score": <число>}
# или если запрос пришел от валидного пользователя admin
# {"score": 42}
# или если произошла ошибка валидации
# {"code": 422, "error": "<сообщение о том какое поле невалидно>"}


# $ curl -X POST  -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "h&f", "method": "online_score", "token": "55cc9ce545bcd144300fe9efc28e65d415b923ebb6be1e19d2750a2c03e80dd209a27954dca045e5bb12418e7d89b6d718a9e35af34e14e1d5bcd5a08f21fc95", "arguments": {"phone": "79175002040", "email": "stupnikov@otus.ru", "first_name": "Стансилав", "last_name": "Ступников", "birthday": "01.01.1990", "gender": 1}}' http://127.0.0.1:8080/method/
# -> {"code": 200, "response": {"score": 5.0}}

# Метод clients_interests.
# Аргументы:
# client_ids - массив числе, обязательно, не пустое
# date - дата в формате DD.MM.YYYY, опционально, может быть пустым

# Валидация аругементов:
# аргументы валидны, если валидны все поля по отдельности.

# Контекст
# в словарь контекста должна прописываться запись  "nclients" - количество id'шников,
# переденанных в запрос


# Ответ:
# в ответ выдается словарь <id клиента>:<список интересов>. Список генерировать произвольно.
# {"client_id1": ["interest1", "interest2" ...], "client2": [...] ...}
# или если произошла ошибка валидации
# {"code": 422, "error": "<сообщение о том какое поле невалидно>"}

# $ curl -X POST  -H "Content-Type: application/json" -d '{"account": "horns&hoofs", "login": "admin", "method": "clients_interests", "token": "d3573aff1555cd67dccf21b95fe8c4dc8732f33fd4e32461b7fe6a71d83c947688515e36774c00fb630b039fe2223c991f045f13f24091386050205c324687a0", "arguments": {"client_ids": [1,2,3,4], "date": "20.07.2017"}}' http://127.0.0.1:8080/method/
# -> {"code": 200, "response": {"1": ["books", "hi-tech"], "2": ["pets", "tv"], "3": ["travel", "music"], "4": ["cinema", "geek"]}}

# Требование: в результате в git должно быть только два(2!) файлика: api.py, test.py.
# Deadline: следующее занятие

import json
import random
import logging
import hashlib
import uuid
import re
from abc import ABCMeta, abstractmethod
from datetime import datetime
from optparse import OptionParser
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


#  ------------------------------------ Field classes ------------------------------------ #

class AbstractField(object):
    __metaclass__ = ABCMeta

    # Declare types explicitly to avoid false positives for 0 and more flexibility
    empty_values = (None, '', [], {}, ())
    __counter = 0

    def __init__(self, required=False, nullable=False):
        self.required = required
        self.nullable = nullable
        cls = self.__class__
        prefix = cls.__name__
        index = cls.__counter
        self.storage_name = '_{}#{}'.format(prefix, index)
        cls.__counter += 1

    def __get__(self, instance, owner):
        if hasattr(instance, self.storage_name):
            return getattr(instance, self.storage_name)
        return None

    def __set__(self, instance, value):
        if value is None and self.required:
            raise ValueError('Field is required.')
        elif value in self.empty_values and not self.nullable:
            raise ValueError('Field not be nullable.')
        elif value:
            value = self.parse_and_validate(value)
        return setattr(instance, self.storage_name, value)

    @abstractmethod
    def parse_and_validate(self, value):
        """Return parsed field value or raise ValueError exception"""


class CharField(AbstractField):
    def parse_and_validate(self, value):
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise ValueError('Field must be a string.')
        return value


class ArgumentsField(AbstractField):
    def parse_and_validate(self, value):
        if not isinstance(value, dict):
            raise ValueError('Field must be a dictionary.')
        return value


class EmailField(AbstractField):
    def parse_and_validate(self, value):
        if not re.match(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)', str(value)):
            raise ValueError('Email address is not valid.')
        return value


class PhoneField(AbstractField):
    def parse_and_validate(self, value):
        if not re.match(r'(^7[\d]{10}$)', str(value)):
            raise ValueError('Phone is not valid.')
        return value


class DateField(AbstractField):
    def parse_and_validate(self, value):
        try:
            value = datetime.strptime(value, '%d.%m.%Y')
        except Exception:
            raise ValueError('Date is not valid.')
        return value


class BirthDayField(DateField):
    def parse_and_validate(self, value):
        value = super(BirthDayField, self).parse_and_validate(value)
        limit_age_years = 70
        current = datetime.now()
        if current.year - value.year > limit_age_years:
            raise ValueError('Age can not be more than {} years.'.format(limit_age_years))
        return value


class GenderField(AbstractField):
    def parse_and_validate(self, value):
        if value not in [UNKNOWN, MALE, FEMALE]:
            raise ValueError('Field value can be only {}, {} or {} and has integer type.'.format(UNKNOWN, MALE, FEMALE))
        return value


class ClientIDsField(AbstractField):
    def parse_and_validate(self, value):
        if not isinstance(value, list):
            raise ValueError('Field must be a list.')
        if not all(isinstance(item, int) for item in value):
            raise ValueError('All clients ids must be an integer type.')
        return value


#  ----------------------------------- Request classes ----------------------------------- #

class RequestMeta(type):
    """Metaclass for request entities with validated fields"""

    def __init__(cls, name, bases, attr_dict):
        super(RequestMeta, cls).__init__(name, bases, attr_dict)
        cls.fields = []
        for key, attr in attr_dict.items():
            if isinstance(attr, AbstractField):
                type_name = type(attr).__name__
                attr.storage_name = '_{}#{}'.format(type_name, key)
                cls.fields.append(key)


class Request(object):
    __metaclass__ = RequestMeta

    def __init__(self, request):
        self._errors = []
        self._fill_fields(**request)

    def _fill_fields(self, **kwargs):
        # Add to kwargs unsent request fields with default None value
        unsent_fields = set(self.fields) - set(kwargs.keys())
        kwargs.update({k: None for k in unsent_fields})
        for k, v in kwargs.items():
            if k in self.fields:
                try:
                    setattr(self, k, v)
                except ValueError as e:
                    self._errors.append('{}: {}'.format(k, e))

    def is_valid(self):
        return not self._errors

    def get_error(self):
        return ', '.join(self._errors)

    def get_not_empty_fields(self):
        fields = []
        for field in self.fields:
            if getattr(self, field) not in AbstractField.empty_values:
                fields.append(field)
        return fields


class ClientsInterestsRequest(Request):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True)


class OnlineScoreRequest(Request):
    _pairs = (
        ('phone', 'email'),
        ('first_name', 'last_name'),
        ('gender', 'birthday'),
    )

    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def _fill_fields(self, **kwargs):
        super(OnlineScoreRequest, self)._fill_fields(**kwargs)
        not_empty_fields = set(self.get_not_empty_fields())
        result = any([not_empty_fields.issuperset(pair) for pair in self._pairs])
        if not result:
            self._errors.append('Pairs {} should be not empty'.format(' or '.join(str(pair) for pair in self._pairs)))


class MethodRequest(Request):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


# ------------------------------- Request handlers classes ------------------------------- #

class RequestHandler(object):
    __metaclass__ = ABCMeta

    def __init__(self, request, ctx):
        self.request = request
        self.ctx = ctx

    @abstractmethod
    def init_request(self, data):
        """ Create request entity here """

    @abstractmethod
    def processing_handler(self, request_data):
        """ Logic of processing the request """

    def run_processing(self, data):
        request_data = self.init_request(data)
        if not request_data.is_valid():
            return request_data.get_error(), INVALID_REQUEST
        return self.processing_handler(request_data)


class OnlineScoreRequestHandler(RequestHandler):
    def init_request(self, data):
        return OnlineScoreRequest(data)

    def processing_handler(self, request_data):
        self.ctx['has'] = request_data.get_not_empty_fields()
        if self.request.is_admin:
            return {'score': 42}, OK
        return {'score': random.randrange(0, 100)}, OK


class ClientsInterestsRequestHandler(RequestHandler):
    def init_request(self, data):
        return ClientsInterestsRequest(data)

    def processing_handler(self, request_data):
        interests = {'books', 'hi-tech', 'pets', 'tv', 'travel', 'music', 'cinema', 'geek'}
        response = {i: random.sample(interests, 2) for i in request_data.client_ids}
        self.ctx['nclients'] = len(request_data.client_ids)
        return response, OK


# ---------------------------------- Base homework part ---------------------------------- #

def check_auth(request):
    if request.login == ADMIN_LOGIN:
        digest = hashlib.sha512(datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).hexdigest()
    else:
        digest = hashlib.sha512(request.account + request.login + SALT).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx):
    handlers = {
        'online_score': OnlineScoreRequestHandler,
        'clients_interests': ClientsInterestsRequestHandler
    }

    request = MethodRequest(request['body'])

    if not request.is_valid():
        return request.get_error(), INVALID_REQUEST

    if not check_auth(request):
        return None, FORBIDDEN

    if request.method not in handlers:
        return None, NOT_FOUND

    return handlers[request.method](request, ctx).run_processing(request.arguments)


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context)
                except Exception, e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return

if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
