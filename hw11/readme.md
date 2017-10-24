## Brenchmarks

### Virtual machine description:
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
### Test data description:
```
> ll -h
[...]
-rw-r--r-- 1 ubuntu ubuntu 102M Oct  2 12:28 20170929000000.tsv.gz
-rw-r--r-- 1 ubuntu ubuntu 102M Oct  2 12:28 20170929000100.tsv.gz
-rw-r--r-- 1 ubuntu ubuntu 102M Oct  2 12:28 20170929000200.tsv.gz
-rw-r--r-- 1 ubuntu ubuntu 102M Oct  2 12:28 20170929000300.tsv.gz
[...]
```
### Python single thread handler execution time:
```
> time python memc_load_single.py --pattern=*.tsv.gz

real  17m30.598s
user  9m13.144s
sys   3m30.204s
```
### Golang multi-thread handler execution time:
```
> time ./memc_load

real  0m58.217s
user  0m37.760s
sys   0m32.948s
```
