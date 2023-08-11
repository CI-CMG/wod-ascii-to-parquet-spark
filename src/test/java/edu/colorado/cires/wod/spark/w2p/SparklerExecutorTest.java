package edu.colorado.cires.wod.spark.w2p;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers
public class SparklerExecutorTest {

  private static final Path TEMP_DIR = Paths.get("target/temp-dir").toAbsolutePath();


  @BeforeEach
  public void before() throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @AfterEach
  public void after() throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @Container
  public LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
      .withServices(S3);

  @Test
  public void testSimple() throws Exception {
    String sourceBucket = "wod-ascii";
    String sourcePrefix = null;
    Set<String> sourceFileSubset = null;
    String outputBucket = "wod-parquet";
    String outputPrefix = null;
    Set<String> datasets = new HashSet<>(Arrays.asList("APB", "CTD"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS", "STD"));

    SparkSession spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.bucket.wod-parquet.access.key", localstack.getAccessKey())
        .config("spark.hadoop.fs.s3a.bucket.wod-parquet.secret.key", localstack.getSecretKey())
        .config("spark.hadoop.fs.s3a.endpoint", localstack.getEndpoint().toString())
        .config("spark.hadoop.fs.s3a.endpoint.region", localstack.getRegion())
        .getOrCreate();
    try {
      S3Client s3 = S3Client.builder()
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
          ))
          .endpointOverride(localstack.getEndpoint())
          .region(Region.of(localstack.getRegion()))
          .build();

      s3.createBucket(c -> c.bucket(sourceBucket));
      s3.createBucket(c -> c.bucket(outputBucket));

      assertTrue(S3Actions.listObjects(s3, outputBucket, null, k -> true).isEmpty());

      s3.putObject(c -> c.bucket(sourceBucket).key("APB/OBS/APBO1997.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/APB/OBS/APBO1997.gz")));
      s3.putObject(c -> c.bucket(sourceBucket).key("CTD/OBS/CTDO1971.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/CTD/OBS/CTDO1971.gz")));
      s3.putObject(c -> c.bucket(sourceBucket).key("CTD/STD/CTDS1967.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/CTD/STD/CTDS1967.gz")));

      SparklerExecutor executor = new SparklerExecutor(spark, s3, sourceBucket, sourcePrefix, TEMP_DIR, sourceFileSubset,
          outputBucket, outputPrefix, datasets, processingLevels, 3, false);
      executor.execute();

      Set<String> keys = S3Actions.listObjects(s3, outputBucket, null, k -> true);
      System.err.println(keys);
      assertTrue(keys.contains("dataset/OBS/WOD_APB_OBS.parquet/_SUCCESS"));
      assertTrue(keys.contains("dataset/OBS/WOD_CTD_OBS.parquet/_SUCCESS"));
      assertTrue(keys.contains("dataset/STD/WOD_CTD_STD.parquet/_SUCCESS"));

      assertTrue(keys.contains("yearly/CTD/STD/CTDS1967.parquet/_SUCCESS"));
      assertTrue(keys.contains("yearly/CTD/OBS/CTDO1971.parquet/_SUCCESS"));
      assertTrue(keys.contains("yearly/APB/OBS/APBO1997.parquet/_SUCCESS"));
    } finally {
      spark.close();
    }


  }

  @Test
  public void testPrefix() throws Exception {

    String bucket = "test-bucket";
    String sourcePrefix = "wod-ascii";
    Set<String> sourceFileSubset = null;
    String outputPrefix = "wod-parquet";
    Set<String> datasets = new HashSet<>(Arrays.asList("APB"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS"));

    SparkSession spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", localstack.getAccessKey())
        .config("spark.hadoop.fs.s3a.secret.key", localstack.getSecretKey())
        .config("spark.hadoop.fs.s3a.endpoint", localstack.getEndpoint().toString())
        .config("spark.hadoop.fs.s3a.endpoint.region", localstack.getRegion())
        .getOrCreate();
    try {
      S3Client s3 = S3Client.builder()
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
          ))
          .endpointOverride(localstack.getEndpoint())
          .region(Region.of(localstack.getRegion()))
          .build();

      s3.createBucket(c -> c.bucket(bucket));

      assertTrue(S3Actions.listObjects(s3, bucket, null, k -> true).isEmpty());

      s3.putObject(c -> c.bucket(bucket).key("wod-ascii/APB/OBS/APBO1997.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/APB/OBS/APBO1997.gz")));
      s3.putObject(c -> c.bucket(bucket).key("wod-ascii/CTD/OBS/CTDO1971.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/CTD/OBS/CTDO1971.gz")));
      s3.putObject(c -> c.bucket(bucket).key("wod-ascii/CTD/STD/CTDS1967.gz"),
          RequestBody.fromFile(new File("src/test/resources/wod/CTD/STD/CTDS1967.gz")));

      SparklerExecutor executor = new SparklerExecutor(spark, s3, bucket, sourcePrefix, TEMP_DIR, sourceFileSubset,
          bucket, outputPrefix, datasets, processingLevels, 3, false);
      executor.execute();

      Set<String> keys = S3Actions.listObjects(s3, bucket, null, k -> true);
      System.out.println(keys);
      assertTrue(keys.contains("wod-parquet/dataset/OBS/WOD_APB_OBS.parquet/_SUCCESS"));
      assertFalse(keys.contains("wod-parquet/dataset/OBS/WOD_CTD_OBS.parquet/_SUCCESS"));
      assertFalse(keys.contains("wod-parquet/dataset/STD/WOD_CTD_STD.parquet/_SUCCESS"));

      assertTrue(keys.contains("wod-parquet/yearly/APB/OBS/APBO1997.parquet/_SUCCESS"));
    } finally {
      spark.close();
    }


  }

}