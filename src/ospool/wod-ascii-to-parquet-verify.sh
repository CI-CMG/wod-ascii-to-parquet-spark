. wod-ascii-to-parquet.conf
OSPOOL=/ospool/${access_point}/data/${username}
home_dir="$PWD"
cd ${OSPOOL}
echo "$PWD"
while read line; do
  wod_line="$line"
  IFS=',' read -r year dataset <<< ${line}
  echo ${dataset}
  echo ${year}
  OUTPUT=${dataset}${year}.tar.gz
  if [ -f "${OSPOOL}/${OUTPUT}" ]; then
          echo "$OUTPUT found"
          tar -xf $OUTPUT
          rm $OUTPUT
   else
           echo "$OUTPUT does not exist."
           echo "$wod_line" >> ${home_dir}/failed-wod-ascii-to-parquet-spark-list.txt
   fi

done < ${home_dir}/wod-ascii-to-parquet-spark-list.txt

cd ${home_dir}
echo "$PWD"
