#!/bin/bash

ssh s3 << end
sudo docker restart -t 0 \$(sudo docker ps -aq)
end

ssh s2 << end
sudo docker restart -t 0 \$(sudo docker ps -aq)
end

sudo docker restart -t 0 $(sudo docker ps -aq)


ssh hive >/dev/null << end
./consumer.sh
exit
end

sleep 5s
echo '----------------------------------------------------------------------------'
cd /home/ubuntu/localtest
wrk -t4 -c1024 -d30s -T5 --script=./wrk.lua --latency http://172.17.0.15:8087/invoke
echo '----------------------------------------------------------------------------'
wrk -t4 -c1024 -d60s -T5 --script=./wrk.lua --latency http://172.17.0.15:8087/invoke




