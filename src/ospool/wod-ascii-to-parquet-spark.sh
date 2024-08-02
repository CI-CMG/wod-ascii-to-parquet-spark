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

wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23%2B9/OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
wget https://downloads.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3-scala2.13.tgz
wget https://cires-cmg-trackline-repository.s3.us-west-2.amazonaws.com/release/edu/colorado/cires/cmg/aws/aws-cli/1.0.1/aws-cli-1.0.1-exe.jar

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
wget -O input/${dataset}/OBS/${file_name} https://www.ncei.noaa.gov/data/oceans/woa/WOD/YEARLY/${dataset}/OBS/${file_name}

mkdir temp
mkdir output

spark-submit \
  --driver-memory=4G \
  --class edu.colorado.cires.wod.spark.w2p.Sparkler \
  wod-ascii-to-parquet-spark-${project.version}.jar \
  -td temp \
  -ib $(pwd)/input \
  -ob $(pwd)/output

java -jar aws-cli-1.0.1-exe.jar s3 cp -r input s3://wod-test-resources/$date_folder/data/ascii
java -jar aws-cli-1.0.1-exe.jar s3 cp -r output s3://wod-test-resources/$date_folder/data/parquet