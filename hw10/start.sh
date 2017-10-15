#!/bin/sh
set -xe

yum install -y  gcc \
				make \
				protobuf \
				protobuf-c \
				protobuf-c-compiler \
				protobuf-c-devel \
				protobuf-python \
				python-devel \
				python-setuptools \
				gdb \
				zlib-devel

ulimit -c unlimited
cd /tmp/otus/
protoc-c --c_out=. deviceapps.proto
protoc --python_out=. deviceapps.proto
python setup.py test
