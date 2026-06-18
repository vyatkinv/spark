#!/bin/bash
set -e

if [ ! -f /data/hdfs/.formatted ]; then
    hdfs namenode -format -force -nonInteractive
    touch /data/hdfs/.formatted
fi

hdfs namenode &
NN_PID=$!
sleep 3

hdfs datanode &
DN_PID=$!
sleep 3

yarn resourcemanager &
RM_PID=$!
sleep 3

yarn nodemanager &
NM_PID=$!

for i in $(seq 1 60); do
    if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
        echo "HDFS left safe mode"
        break
    fi
    sleep 1
done

echo "READY"

wait -n $NN_PID $DN_PID $RM_PID $NM_PID
