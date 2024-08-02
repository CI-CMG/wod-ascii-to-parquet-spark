#!/bin/bash

set -e

if [ -f wod-ascii-to-parquet-spark-list.txt ]; then
  rm wod-ascii-to-parquet-spark-list.txt
  touch wod-ascii-to-parquet-spark-list.txt
else
  touch wod-ascii-to-parquet-spark-list.txt
fi

if [ -f files.tsv ]; then
  rm files.tsv
fi

wget -r -np -R "*" --rejected-log=files.tsv  https://www.ncei.noaa.gov/data/oceans/woa/WOD/YEARLY/
rm -rf 'www.ncei.noaa.gov'

while IFS=$'\t' read -r -a myArray
do
  path="${myArray[5]}"
  if [[ $path =~ ^data/oceans/woa/WOD/YEARLY/.+/OBS/.*\.gz$ ]]; then
    if [[ $path =~ ^data/oceans/woa/WOD/YEARLY/SUR/.*$ ]]; then
      dataset=SUR
      year=ALL
    else
      dataset=${path:35:3}
      year=${path:39:4}
    fi
    echo "$year,$dataset" >> wod-ascii-to-parquet-spark-list.txt
  fi
done < files.tsv

awk -i inplace '!seen[$0]++' wod-ascii-to-parquet-spark-list.txt

rm files.tsv