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

Edit the wod-ascii-to-parquet.conf file and set the AWS credentials
```bash
vim wod-ascii-to-parquet.conf
```

Execute the job
```bash
condor_submit wod-ascii-to-parquet-spark.submit
```

#### Useful commands

Check the status of a job
```bash
condor_q -nobatch
```

Cancel all jobs
```bash
condor_rm <username>
```





