#!/bin/bash

set -ex

bucket="$1"
s3_access_key="$2"
s3_secret_key="$3"
external_host_ip="$4"
internal_host_ip="$5"

/spark-3.4.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --driver-memory=6G \
  --executor-memory 3g \
  --num-executors 100 \
  --conf spark.driver.host=$external_host_ip \
  --conf spark.driver.port=8088 \
  --conf spark.driver.bindAddress=$internal_host_ip \
  --conf spark.blockManager.port=40332 \
  --conf spark.hadoop.fs.s3a.access.key=$s3_access_key \
  --conf spark.hadoop.fs.s3a.secret.key=$s3_secret_key \
  --conf spark.hadoop.fs.s3a.endpoint.region us-east-1 \
  --master spark://$external_host_ip:7077 \
  --class edu.colorado.cires.wod.spark.w2p.Sparkler \
  wod-ascii-to-parquet-spark-1.1.0-SNAPSHOT.jar \
  -td temp \
  -ib $bucket \
  -ip
  -ob $bucket

/spark-3.4.3-bin-hadoop3-scala2.13/bin/spark-submit --driver-memory 1g --executor-memory 1g --num-executors 1 --master spark://35.89.244.178:7077  --conf spark.driver.host=35.89.244.178 --conf spark.driver.port=8088 --conf spark.driver.bindAddress=172.31.21.220 --conf spark.blockManager.port=40332 --class foo.HelloWorld hello-world-spark-1.0-SNAPSHOT.jar