#!/bin/bash

source activate cl

echo '1. starting mysql...'

sudo -S service mysql start

echo '2. starting redis...'
redis-server /etc/redis/redis.conf

echo '3. starting pm2 jobs...'
pm2 start /root/pm2.config.js

# 开机启动服务成功
echo $(date "+%Y-%m-%d %H:%M:%S") start success... >&1 | tee -a /usr/local/start.log

cd /root/app/web
/usr/local/anaconda3/envs/cl/bin/python manage.py runserver 0.0.0.0:8000