package edu.colorado.cires.wod.spark.w2p;

import static edu.colorado.cires.wod.spark.w2p.S3Actions.listObjects;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;

public class DatasetTrain {

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
  private final FileSystemType ifs;
  private final FileSystemType ofs;


  public DatasetTrain(SparkSession spark, S3Client s3, String dataset, String sourceBucket, String sourcePrefix, Path tempDir,
      String processingLevel, Set<String> sourceFileSubset, String outputBucket, String outputPrefix, boolean overwrite, int batchSize, FileSystemType ifs, FileSystemType ofs) {
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
    this.ifs = ifs;
    this.ofs = ofs;
  }


  public List<DatasetYearTrain> plan() {
    String keyPrefix = resolvePrefix();
    Predicate<String> filter = resolveFilter();
    Set<String> keys = listObjects(ifs, s3, sourceBucket, keyPrefix, filter);
    return keys.stream()
        .map(key -> {
          TransformationErrorHandler transformationErrorHandler = new TransformationErrorHandler(spark, dataset, processingLevel, outputBucket, outputPrefix, key, ofs);
          return new DatasetYearTrain(spark, s3, dataset, sourceBucket, tempDir, processingLevel, outputBucket, outputPrefix, key, overwrite, batchSize, transformationErrorHandler,
              ifs, ofs);
        })
        .collect(Collectors.toList());
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
