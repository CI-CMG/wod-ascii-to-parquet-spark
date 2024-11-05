#!/bin/bash

set -e

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
  parquet_dir=SUR_ALL.parquet
else
  file_name=${dataset}O${year}.gz
  parquet_dir=${dataset}O${year}.parquet
fi

mkdir -p input/${dataset}/OBS
mkdir -p ${date_folder}/data/ascii/${dataset}/OBS

java -cp wod-ascii-to-parquet-spark-2.1.0-SNAPSHOT.jar edu.colorado.cires.wod.spark.w2p.OsPoolUtils http-download --url https://www.ncei.noaa.gov/data/oceans/woa/WOD/YEARLY/${dataset}/OBS/${file_name} -o ${date_folder}/data/ascii/${dataset}/OBS/${file_name}
mkdir temp
mkdir ${date_folder}/data/parquet

spark-submit \
  --master 'local[4]' \
  --conf spark.cores.max=4 \
  --conf spark.ui.enabled=false \
  --conf "spark.local.dir=$(pwd)/temp" \
  --driver-memory=7G \
  --driver-java-options '-XX:ActiveProcessorCount=4' \
  --class edu.colorado.cires.wod.spark.w2p.Sparkler \
  wod-ascii-to-parquet-spark-${project.version}.jar \
  -bs 2000 \
  -td temp \
  -ib $(pwd)/${date_folder}/data/ascii \
  -ob $(pwd)/${date_folder}/data/parquet

cd ${date_folder}/data/parquet/yearly/${dataset}/OBS
tar -czf ${dataset}${year}.parquet.tar.gz ${parquet_dir}

cd "$working_dir"
tar -czf output.tar.gz ${date_folder}