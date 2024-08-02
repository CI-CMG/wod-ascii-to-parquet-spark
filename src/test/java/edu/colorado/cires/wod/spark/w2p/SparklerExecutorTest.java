package edu.colorado.cires.wod.spark.w2p;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import io.findify.s3mock.S3Mock;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class SparklerExecutorTest {

  private static final Path TEMP_DIR = Paths.get("target/temp-dir").toAbsolutePath();

  private S3Mock s3Mock;
  private SparkSession spark;
  private S3Client s3;


  @BeforeEach
  public void before() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    s3Mock = new S3Mock.Builder().withInMemoryBackend().withPort(8001).build();
    s3Mock.start();
    spark = SedonaContext.create(SedonaContext
        .builder()
        .appName("test")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", "foo")
        .config("spark.hadoop.fs.s3a.secret.key", "bar")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:8001")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "warn")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .getOrCreate());
    s3 = S3Client.builder()
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(true)
            .build())
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .endpointOverride(new URI("http://localhost:8001"))
        .region(Region.US_EAST_1)
        .build();
  }

  @AfterEach
  public void after() throws IOException {
    spark.close();
    s3Mock.shutdown();
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @Test
  public void testSimple() throws Exception {

    String sourceBucket = "wod-ascii";
    String sourcePrefix = null;
    Set<String> sourceFileSubset = null;
    String outputBucket = "wod-parquet";
    String outputPrefix = null;
    Set<String> datasets = new HashSet<>(Arrays.asList("APB", "CTD"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS", "STD"));

    s3.createBucket(c -> c.bucket(sourceBucket));
    s3.createBucket(c -> c.bucket(outputBucket));

    assertTrue(S3Actions.listObjects(FileSystemType.s3, s3, outputBucket, null, k -> true).isEmpty());

    s3.putObject(c -> c.bucket(sourceBucket).key("APB/OBS/APBO1997.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/APB/OBS/APBO1997.gz")));
    s3.putObject(c -> c.bucket(sourceBucket).key("CTD/OBS/CTDO1971.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/CTD/OBS/CTDO1971.gz")));
    s3.putObject(c -> c.bucket(sourceBucket).key("CTD/STD/CTDS1967.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/CTD/STD/CTDS1967.gz")));

    SparklerExecutor executor = new SparklerExecutor(spark, s3, sourceBucket, sourcePrefix, TEMP_DIR, sourceFileSubset,
        outputBucket, outputPrefix, datasets, processingLevels, false, 1000, FileSystemType.s3, FileSystemType.s3);
    executor.execute();

    Set<String> keys = S3Actions.listObjects(FileSystemType.s3, s3, outputBucket, null, k -> true);

    assertTrue(keys.contains("yearly/CTD/STD/CTDS1967.parquet/_SUCCESS"));
    assertTrue(keys.contains("yearly/CTD/OBS/CTDO1971.parquet/_SUCCESS"));
    assertTrue(keys.contains("yearly/APB/OBS/APBO1997.parquet/_SUCCESS"));


  }

  @Test
  public void testPrefix() throws Exception {

    String bucket = "test-bucket";
    String sourcePrefix = "wod-ascii";
    Set<String> sourceFileSubset = null;
    String outputPrefix = "wod-parquet";
    Set<String> datasets = new HashSet<>(Arrays.asList("APB"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS"));

    s3.createBucket(c -> c.bucket(bucket));

    assertTrue(S3Actions.listObjects(FileSystemType.s3, s3, bucket, null, k -> true).isEmpty());

    s3.putObject(c -> c.bucket(bucket).key("wod-ascii/APB/OBS/APBO1997.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/APB/OBS/APBO1997.gz")));
    s3.putObject(c -> c.bucket(bucket).key("wod-ascii/CTD/OBS/CTDO1971.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/CTD/OBS/CTDO1971.gz")));
    s3.putObject(c -> c.bucket(bucket).key("wod-ascii/CTD/STD/CTDS1967.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/CTD/STD/CTDS1967.gz")));

    SparklerExecutor executor = new SparklerExecutor(spark, s3, bucket, sourcePrefix, TEMP_DIR, sourceFileSubset,
        bucket, outputPrefix, datasets, processingLevels, false, 1000, FileSystemType.s3, FileSystemType.s3);
    executor.execute();

    Set<String> keys = S3Actions.listObjects(FileSystemType.s3, s3, bucket, null, k -> true);

    assertTrue(keys.contains("wod-parquet/yearly/APB/OBS/APBO1997.parquet/_SUCCESS"));

  }

  @Test
  public void testPrefixSUR() throws Exception {

    String bucket = "test-bucket";
    String sourcePrefix = "wod-ascii";
    Set<String> sourceFileSubset = null;
    String outputPrefix = "wod-parquet";
    Set<String> datasets = new HashSet<>(Arrays.asList("SUR"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS"));

    s3.createBucket(c -> c.bucket(bucket));

    assertTrue(S3Actions.listObjects(FileSystemType.s3, s3, bucket, null, k -> true).isEmpty());

    s3.putObject(c -> c.bucket(bucket).key("wod-ascii/SUR/OBS/SURF_ALL.gz"),
        RequestBody.fromFile(new File("src/test/resources/wod/SUR/OBS/SURF_ALL.gz")));

    SparklerExecutor executor = new SparklerExecutor(spark, s3, bucket, sourcePrefix, TEMP_DIR, sourceFileSubset,
        bucket, outputPrefix, datasets, processingLevels, false, 1000, FileSystemType.s3, FileSystemType.s3);
    executor.execute();

    Set<String> keys = S3Actions.listObjects(FileSystemType.s3, s3, bucket, null, k -> true);

    assertTrue(keys.contains("wod-parquet/yearly/SUR/OBS/SUR_ALL.parquet/_SUCCESS"));

  }

}