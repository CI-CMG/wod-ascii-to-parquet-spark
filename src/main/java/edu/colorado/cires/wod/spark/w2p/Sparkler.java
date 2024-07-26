package edu.colorado.cires.wod.spark.w2p;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;
import java.util.TreeSet;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@Command(
    name = "wod-ascii-to-parquet-spark",
    description = "Executes Spark jobs to convert WOD gzipped ASCII files to Parquet",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class
)
public class Sparkler implements Serializable, Runnable {

  private static final long serialVersionUID = 0L;

  @Option(names = {"-ib", "--input-bucket"}, required = true, description = "The input S3 bucket containing compressed ASCII WOD files")
  private String sourceBucket;
  @Option(names = {"-ibr", "--input-bucket-region"}, required = false, description = "The input S3 bucket region")
  private String sourceBucketRegion;
  @Option(names = {"-ob", "--output-bucket"}, required = true, description = "The output S3 bucket where to put converted Parquet files")
  private String outputBucket;
  @Option(names = {"-obr", "--output-bucket-region"}, required = false, description = "The output S3 bucket region")
  private String outputBucketRegion;
  @Option(names = {"-ds", "--data-set"}, required = true, split = ",", defaultValue = "APB,CTD,DRB,GLD,MBT,MRB,OSD,PFL,SUR,UOR,XBT", description = "A comma separated list of data codes - Default: ${DEFAULT-VALUE}")
  private List<String> datasets;
  @Option(names = {"-p", "--processing-level"}, required = true, split = ",", defaultValue = "OBS", description = "A comma separated list of processing levels - Default: ${DEFAULT-VALUE}")
  private List<String> processingLevels;
  @Option(names = {"-o", "--overwrite"}, description = "Overwrite existing parquet stores if they already exist")
  private boolean overwrite = false;

  @Option(names = {"-ip", "--input-prefix"}, description = "An optional key prefix of where to read input files if not in the root of the input bucket")
  private String sourcePrefix;
  @Option(names = {"-op", "--output-prefix"}, description = "An optional key prefix of where to write output files if not in the root of the output bucket")
  private String outputPrefix;
  @Option(names = {"-s", "--subset"}, split = ",", description = "A comma separated list file names to process. If omitted all files defined by the dataset and processing levels will be processed")
  private List<String> sourceFileSubset;
  @Option(names = {"-td", "--temp-directory"}, description = "A working directory where input files can be placed while processing. Defaults to the \"java.io.tmpdir\" directory")
  private String tempDir = Paths.get(System.getProperty("java.io.tmpdir")).toString();

  @Option(names = {"-ia", "--input-access"}, description = "An optional access key for the input bucket")
  private String sourceAccessKey;
  @Option(names = {"-is", "--input-secret"}, description = "An optional secret key for the input bucket")
  private String sourceSecretKey;

  @Option(names = {"-oa", "--output-access"}, description = "An optional access key for the output bucket")
  private String outputAccessKey;
  @Option(names = {"-os", "--output-secret"}, description = "An optional secret key for the output bucket")
  private String outputSecretKey;

  @Option(names = {"-bs", "--batch-size"}, description = "Number of casts to insert per batch")
  private int batchSize = 10000;

  @Option(names = {"-ifs", "--input-file-system"}, description = "local, s3, emr23 (Optimized S3 access for EMR)", defaultValue = "local")
  private FileSystemType ifs = FileSystemType.local;

  @Option(names = {"-ofs", "--output-file-system"}, description = "local, s3, emr23 (Optimized S3 access for EMR)", defaultValue = "local")
  private FileSystemType ofs = FileSystemType.local;

  @Override
  public void run() {
    SparkSession.Builder sparkBuilder = SedonaContext.builder();
    if (outputBucketRegion != null) {
      sparkBuilder.config("spark.hadoop.fs.s3a.endpoint.region", outputBucketRegion);
    }
    if (outputAccessKey != null) {
      sparkBuilder.config("spark.hadoop.fs.s3a.access.key", outputAccessKey);
    }
    if (outputSecretKey != null) {
      sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", outputSecretKey);
    }
    SparkSession spark = SedonaContext.create(sparkBuilder.getOrCreate());

    spark.sparkContext().addSparkListener(new SparkListener() {

      @Override
      public void onJobEnd(SparkListenerJobEnd jobEnd) {
        JobResult result = jobEnd.jobResult();
        if (result instanceof JobFailed) {
          System.err.println("Failed job detected. Exiting.");
          spark.sparkContext().stop(1);
        }
      }
    });

    S3Client s3 = null;
    if (ifs != FileSystemType.local || ofs != FileSystemType.local) {
      S3ClientBuilder s3Builder = S3Client.builder();
      if (outputAccessKey != null) {
        s3Builder.credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(outputAccessKey, outputSecretKey)
        ));
      }
      s3Builder.region(Region.of(outputBucketRegion));
      s3 = s3Builder.build();
    }


    SparklerExecutor executor = new SparklerExecutor(
        spark,
        s3,
        sourceBucket,
        sourcePrefix,
        Paths.get(tempDir),
        sourceFileSubset == null ? null : new TreeSet<>(sourceFileSubset),
        outputBucket,
        outputPrefix,
        new TreeSet<>(datasets),
        new TreeSet<>(processingLevels),
        overwrite, batchSize, ifs, ofs);
    try {
      executor.execute();
    } catch (IOException e) {
      throw new IllegalStateException("An error occurred while processing", e);
    }
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Sparkler()).execute(args));
  }

}
