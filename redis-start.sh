#!/bin/bash

apt-get install redis-server -y
/etc/init.d/redis-server stop

mkdir -p /var/lib/redis/6400
mkdir -p /var/run/redis

redis-server $(pwd)/redis-config/6400.conf
echo 'Service 6400 has been started'

