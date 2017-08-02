## Описание Web-сервера

### Архитектура
В основе реализации - **Multi-threaded** на **N** воркерах. На тестах данная реализация показала наилучшие результаты по скорости и отказоустойчивости.

Воркеры реализованны на основе процессов с помощью модуля `multiprocessing`. На каждый запрос пользователя внутри выбранного процесса создается новый поток, в котором происходит обработка запроса и возвращается ответ.

Логика сервера реализована в 2-х классах:

`HTTPThreadingServer` - создает сокет, слушает входящие соединения, создает потоки.

`HTTPRequestHandler` - принимает запрос, парсит заголовки, форирует и возвращает ответ.

### Тестирование
Тестирование осуществлялось на виртуальной машине со следующими характеристиками:

```
[...]
Description : Ubuntu 16.04.2 LTS
Release     : 16.04
Codename    : xenial
[...]
processor   : 0
vendor_id   : GenuineIntel
cpu family  : 6
model       : 61
model name  : Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
[...]
processor   : 1
vendor_id   : GenuineIntel
cpu family  : 6
model       : 61
model name  : Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
[...]
MemTotal    : 2048156 kB
[...]
```

Результаты тестирования на 2-х воркерах (наилучшая конфигурация):

```
python httpd.py -s=127.0.0.1 -p=8080 -w=2
```
```
ab -n 50000 -c 100 -r -s 60 http://127.0.0.1:8080/test.html
```
```
This is ApacheBench, Version 2.3 <$Revision: 1706008 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking 127.0.0.1 (be patient)
Completed 5000 requests
Completed 10000 requests
Completed 15000 requests
Completed 20000 requests
Completed 25000 requests
Completed 30000 requests
Completed 35000 requests
Completed 40000 requests
Completed 45000 requests
Completed 50000 requests
Finished 50000 requests

Server Software:         OtusServer
Server Hostname:         127.0.0.1
Server Port:             8080

Document Path:           /test.html
Document Length:         97 bytes

Concurrency Level:       100
Time taken for tests:    53.685 seconds
Complete requests:       50000
Failed requests:         0
Total transferred:       10900000 bytes
HTML transferred:        4850000 bytes
Requests per second:     931.36 [#/sec] (mean)
Time per request:        107.370 [ms] (mean)
Time per request:        1.074 [ms] (mean, across all concurrent requests)
Transfer rate:           198.28 [Kbytes/sec] received

Connection Times (ms)
                      min  mean[+/-sd]  median  max
Connect:              0    0     0.1    0       3
Processing:           1    107   81.5   112     270
Waiting:              1    107   81.5   112     270
Total:                2    107   81.5   112     270

Percentage of the requests served within a certain time (ms)
  50%    112
  66%    175
  75%    189
  80%    194
  90%    206
  95%    215
  98%    227
  99%    235
 100%    270 (longest request)
```