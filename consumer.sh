#!/bin/bash
cd ~/localtest/debian-jdk8-adaptive-loadbalance/debian-jdk8-consumer
docker kill $(docker ps -q)
docker rm $(docker container ls -aq)
docker rmi consumer
docker build --build-arg user_code_address=https://code.aliyun.com/xxx/adaptive-loadbalance.git -t consumer .
docker run -td  -p 8087:8087 --cpu-period=50000 --cpu-quota=200000 --name=consumer --network=host consumer
docker ps -a

