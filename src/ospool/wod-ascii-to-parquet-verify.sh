#!/bin/bash

set -e

. wod-ascii-to-parquet.conf
export AWS_ACCESS_KEY_ID="$aws_access_key_id"
export AWS_SECRET_ACCESS_KEY="$aws_secret_access_key"
export AWS_REGION=us-east-1

date_folder=$(date +%Y-%m)

java -cp wod-ascii-to-parquet-spark-${project.version}.jar edu.colorado.cires.wod.spark.w2p.OsPoolUtils s3-list-missing -b s3://wod-test-resources/$date_folder/data/ascii -l original-wod-ascii-to-parquet-spark-list.txt -o failed-wod-ascii-to-parquet-spark-list.txt
