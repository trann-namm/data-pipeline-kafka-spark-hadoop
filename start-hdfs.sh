#!/bin/bash
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
    sleep 10
    hdfs dfs -chown -R trannam:trannam /
    hdfs dfs -chmod 755 /
    hdfs dfsadmin -safemode leave
fi
hdfs namenode
hdfs dfs -chown -R trannam:trannam /
hdfs dfs -chmod 755 /
hdfs dfsadmin -safemode leave