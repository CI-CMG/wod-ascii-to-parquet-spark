package edu.colorado.cires.wod.spark.w2p;

import static edu.colorado.cires.wod.spark.w2p.S3Actions.deletePrefix;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.listObjects;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;

public class DatasetTrain {

  public static final int MAX_RECORDS_PER_FILE = 20000;

  private final SparkSession spark;
  private final S3Client s3;
  private final String dataset;
  private final String sourceBucket;
  private final String sourcePrefix;
  private final Path tempDir;
  private final String processingLevel;
  private final Set<String> sourceFileSubset;
  private final String outputBucket;
  private final String outputPrefix;
  private final boolean overwrite;
  private final int batchSize;
  private final boolean emr;


  public DatasetTrain(SparkSession spark, S3Client s3, String dataset, String sourceBucket, String sourcePrefix, Path tempDir,
      String processingLevel, Set<String> sourceFileSubset, String outputBucket, String outputPrefix, boolean overwrite, int batchSize, boolean emr) {
    this.batchSize = batchSize;
    this.spark = spark;
    this.s3 = s3;
    this.dataset = dataset;
    this.sourceBucket = sourceBucket;
    this.sourcePrefix = sourcePrefix;
    this.tempDir = tempDir;
    this.processingLevel = processingLevel;
    this.sourceFileSubset = sourceFileSubset;
    this.outputBucket = outputBucket;
    this.outputPrefix = outputPrefix;
    this.overwrite = overwrite;
    this.emr = emr;
  }


  public List<DatasetYearTrain> plan() {
    String keyPrefix = resolvePrefix();
    Predicate<String> filter = resolveFilter();
    Set<String> keys = listObjects(s3, sourceBucket, keyPrefix, filter);
    return keys.stream()
        .map(key -> {
          TransformationErrorHandler transformationErrorHandler = new TransformationErrorHandler(spark, dataset, processingLevel, outputBucket, outputPrefix, key,
              emr);
          return new DatasetYearTrain(this, spark, s3, dataset, sourceBucket, tempDir, processingLevel, outputBucket, outputPrefix, key, overwrite, batchSize, transformationErrorHandler,
              emr);
        })
        .collect(Collectors.toList());
  }

  public void accumulate(List<String> datasetYearKeys) {
    String key = resolveKey();
    String parquetFile = resolveParquetFile(key);
    deletePrefix(s3, outputBucket, key + "/");
    if (!datasetYearKeys.isEmpty()) {
      List<Callable<Dataset<Cast>>> tasks = datasetYearKeys.stream()
          .map(k -> new Callable<Dataset<Cast>>() {

            @Override
            public Dataset<Cast> call() throws Exception {
              return spark.read().parquet(k).as(Encoders.bean(Cast.class));
            }
          })
          .collect(Collectors.toList());
      List<Dataset<Cast>> datasets;
      ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
      try {
        List<Future<Dataset<Cast>>> done = tasks.stream().map(executor::submit).collect(Collectors.toList());
        datasets = done.stream()
            .map(f -> {
              try {
                return f.get();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while getting dataset to union", e);
              } catch (ExecutionException e) {
                throw new RuntimeException("An error occurred getting dataset to union", e);
              }
            }).collect(Collectors.toList());
      } finally {
        executor.shutdown();
        try {
          if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
            executor.shutdownNow();
          }
        } catch (InterruptedException e) {
          executor.shutdownNow();
        }
      }

      Dataset<Cast> sparkDs = datasets.get(0);
      for (int i = 1; i < datasetYearKeys.size(); i++) {
        sparkDs = sparkDs.union(datasets.get(i));
      }
      sparkDs
          .write()
          .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
          .format("parquet")
          .mode("overwrite")
          .partitionBy("geohash", "year")
          .save(parquetFile);
    }
  }

  private String resolveParquetFile(String key) {
    return new StringBuilder(emr ? "s3://" : "s3a://").append(outputBucket).append("/").append(key).toString();
  }

  private String resolveKey() {
    StringBuilder sb = new StringBuilder();
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }
    sb.append("dataset/").append(processingLevel).append("/")
        .append("WOD_").append(dataset).append("_").append(processingLevel).append(".parquet");
    return sb.toString();
  }

  private String resolvePrefix() {
    StringBuilder keyPrefix = new StringBuilder();
    if (sourcePrefix != null) {
      keyPrefix.append(sourcePrefix.replaceAll("/+$", "")).append("/");
    }
    keyPrefix.append(dataset).append("/").append(processingLevel).append("/");
    return keyPrefix.toString();
  }

  private Predicate<String> resolveFilter() {
    List<Predicate<String>> allPredicates = new ArrayList<>();

    if (sourceFileSubset != null && !sourceFileSubset.isEmpty()) {
      Predicate<String> fileFilter = sourceFileSubset.stream()
          .map(file -> (Predicate<String>) key -> key.endsWith("/" + file))
          .reduce(key -> false, Predicate::or);
      allPredicates.add(fileFilter);
    } else {
      allPredicates.add(key -> key.endsWith(".gz"));
    }

    return allPredicates.stream().reduce(key -> true, Predicate::and);
  }

}
