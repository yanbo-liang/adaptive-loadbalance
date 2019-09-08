#!/bin/bash
cd ~/localtest/debian-jdk8-adaptive-loadbalance/debian-jdk8-provider
docker kill $(docker ps -q)
docker rm $(docker container ls -aq)
docker rmi provider
docker build --build-arg user_code_address=https://code.aliyun.com/xxx/adaptive-loadbalance.git -t provider .
docker run -td  -p 20890:20890 --cpu-period=50000 --cpu-quota=150000 --name=provider-large provider provider-large
docker ps -a

