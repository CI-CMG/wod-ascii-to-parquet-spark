# NOAA / NCEI World Ocean Database ASCII To Parquet Spark Conversion Job

## Introduction

This project defines an Apache Spark Job that will convert ASCII World Ocean
Database files to the Parquet format defined by https://github.com/CI-CMG/wod-parquet-model.

## Build
```bash
mvn clean install
```

## Usage 

### OSPool

#### Initial setup (On the OSPool gateway)

This job requires some resources to be present on the OSPool access point.  These will be copied to the workers
when the job is submitted.  Run the following if these files do not exist.

```bash
wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.23%2B9/OpenJDK11U-jre_x64_linux_hotspot_11.0.23_9.tar.gz
wget https://downloads.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3-scala2.13.tgz
wget https://cires-cmg-trackline-repository.s3.us-west-2.amazonaws.com/release/edu/colorado/cires/cmg/aws/aws-cli/1.0.1/aws-cli-1.0.1-exe.jar
```

#### On your laptop

Copy the zip file to your OSPool gateway (assuming you have an ospool SSH alias)
```bash
scp wod-ascii-to-parquet-spark-1.1.0.zip ospool:~/
```

SSH into the OSPool gateway (assuming you have an ospool SSH alias)
```bash
ssh ospool
```

#### Submit the job (On the OSPool gateway)

Unzip the bundle
```bash
unzip wod-ascii-to-parquet-spark-1.1.0.zip
```

Build the job list
```bash
./wod-ascii-to-parquet-build-list.sh
```

Edit the wod-ascii-to-parquet.conf file and set the username and access_point
Note: can not contain spaces
```bash
vim wod-ascii-to-parquet.conf
```

Edit the wod-ascii-to-parquet-spark.submit and set the username and access_point
Note: can not contain spaces
```bash
username =
access_point =
```

Execute the job
```bash
condor_submit wod-ascii-to-parquet-spark.submit
```

When the job is done, you should check that it all files completed successfully.

```bash:
cp wod-ascii-to-parquet-spark-list.txt original-wod-ascii-to-parquet-spark-list.txt
./wod-ascii-to-parquet-verify.sh
```
The wod-ascii-to-parquet-verify.sh reads the original-wod-ascii-to-parquet-spark-list.txt and compares it
with the files in the S3 bucket.  An output file called failed-wod-ascii-to-parquet-spark-list.txt will
be created.  If there are values in this file, investigation will need to be done
to determine the cause.  The OSPool output logs are useful. Also check for held jobs.

To rerun these failed files
```bash
mv failed-wod-ascii-to-parquet-spark-list.txt wod-ascii-to-parquet-spark-list.txt
condor_submit wod-ascii-to-parquet-spark.submit
```


#### Othe useful commands

Check the status of a job
```bash
condor_q -nobatch
```

Check held jobs
```bash
condor_q -hold
```

Cancel all jobs
```bash
condor_rm <username>
```





