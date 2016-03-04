#!/bin/bash

apt-get install python-dev -y
apt-get install python-pip -y

pip install singledispatch
pip install backports.ssl_match_hostname
pip install backports-abc
pip install certifi

apt-get install protobuf-compiler -y
SRC_DIR=$(pwd)/tornado/proto
DST_DIR=$(pwd)/tornado/proto
protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/mqttmessage.proto

apt-get install python-protobuf -y
pip install redis
