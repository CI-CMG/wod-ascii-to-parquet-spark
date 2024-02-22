package edu.colorado.cires.wod.spark.w2p;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
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
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class SparklerExecutorLocalTest {

  private static final Path TEMP_DIR = Paths.get("target/temp-dir").toAbsolutePath();
  private static final Path BUCKET_DIR = Paths.get("target/local-bucket").toAbsolutePath();

  @BeforeEach
  public void before() throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    FileUtils.deleteQuietly(BUCKET_DIR.toFile());
    Files.createDirectories(BUCKET_DIR);
  }

  @AfterEach
  public void after() throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    FileUtils.deleteQuietly(BUCKET_DIR.toFile());
  }

  @Test
  public void test() throws Exception {
    String sourceBucket = BUCKET_DIR.toString();
    String sourcePrefix = "ascii";
    Set<String> sourceFileSubset = null;
    String outputBucket = sourceBucket;
    String outputPrefix = "parquet";
    Set<String> datasets = new HashSet<>(Arrays.asList("APB", "CTD"));
    Set<String> processingLevels = new HashSet<>(Arrays.asList("OBS", "STD"));

    Files.createDirectories(BUCKET_DIR.resolve(sourcePrefix).resolve("APB/OBS"));
    Files.copy(Paths.get("src/test/resources/wod/APB/OBS/APBO1997.gz"), BUCKET_DIR.resolve(sourcePrefix).resolve("APB/OBS/APBO1997.gz"));

    Files.createDirectories(BUCKET_DIR.resolve(sourcePrefix).resolve("CTD/OBS"));
    Files.copy(Paths.get("src/test/resources/wod/CTD/OBS/CTDO1971.gz"), BUCKET_DIR.resolve(sourcePrefix).resolve("CTD/OBS/CTDO1971.gz"));

    Files.createDirectories(BUCKET_DIR.resolve(sourcePrefix).resolve("CTD/STD"));
    Files.copy(Paths.get("src/test/resources/wod/CTD/STD/CTDS1967.gz"), BUCKET_DIR.resolve(sourcePrefix).resolve("CTD/STD/CTDS1967.gz"));

    try (SparkSession spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()) {

      SparklerExecutor executor = new SparklerExecutor(
          spark,
          null,
          sourceBucket,
          sourcePrefix,
          TEMP_DIR,
          sourceFileSubset,
          outputBucket,
          outputPrefix,
          datasets,
          processingLevels,
          3,
          false,
          1000,
          FileSystemType.local
      );
      executor.execute();

    }

    Set<String> keys = S3Actions.listObjects(FileSystemType.local, null, outputBucket, null, k -> true);
    System.err.println(keys);
//    assertTrue(keys.contains("parquet/dataset/OBS/WOD_APB_OBS.parquet/_SUCCESS"));
//    assertTrue(keys.contains("parquet/dataset/OBS/WOD_CTD_OBS.parquet/_SUCCESS"));
//    assertTrue(keys.contains("parquet/dataset/STD/WOD_CTD_STD.parquet/_SUCCESS"));

    assertTrue(keys.contains("parquet/yearly/CTD/STD/CTDS1967.parquet/_SUCCESS"));
    assertTrue(keys.contains("parquet/yearly/CTD/OBS/CTDO1971.parquet/_SUCCESS"));
    assertTrue(keys.contains("parquet/yearly/APB/OBS/APBO1997.parquet/_SUCCESS"));

  }


}