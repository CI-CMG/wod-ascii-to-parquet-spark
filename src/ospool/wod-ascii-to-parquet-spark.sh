#!/bin/bash

set -e

. wod-ascii-to-parquet.conf
export AWS_ACCESS_KEY_ID="$aws_access_key_id"
export AWS_SECRET_ACCESS_KEY="$aws_secret_access_key"
export AWS_REGION=us-east-1

set -x

year="$1"
dataset="$2"
date_folder=$(date +%Y-%m)

tar -xvf OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
tar -xvf spark-3.4.3-bin-hadoop3-scala2.13.tgz

export JAVA_HOME="$PWD/jdk-11.0.23+9-jre"
export SPARK_HOME="$PWD/spark-3.4.3-bin-hadoop3-scala2.13"
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"

if [[ $dataset = 'SUR' ]]; then
  file_name=SURF_ALL.gz
else
  file_name=${dataset}O${year}.gz
fi

mkdir -p input/${dataset}/OBS

java -cp wod-ascii-to-parquet-spark-${project.version}.jar edu.colorado.cires.wod.spark.w2p.OsPoolUtils http-download --url https://www.ncei.noaa.gov/data/oceans/woa/WOD/YEARLY/${dataset}/OBS/${file_name} -o input/${dataset}/OBS/${file_name}

mkdir temp
mkdir output

spark-submit \
  --master 'local[4]' \
  --conf spark.cores.max=4 \
  --driver-memory=7G \
  --driver-java-options '-XX:ActiveProcessorCount=4' \
  --class edu.colorado.cires.wod.spark.w2p.Sparkler \
  wod-ascii-to-parquet-spark-${project.version}.jar \
  -bs 2000 \
  -td temp \
  -ib $(pwd)/input \
  -ob $(pwd)/output

java -jar aws-cli-1.0.1-exe.jar s3 cp -r output s3://wod-test-resources/$date_folder/data/parquet
java -jar aws-cli-1.0.1-exe.jar s3 cp -r input s3://wod-test-resources/$date_folder/data/ascii