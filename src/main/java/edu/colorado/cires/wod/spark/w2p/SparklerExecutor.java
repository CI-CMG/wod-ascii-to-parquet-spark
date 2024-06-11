package edu.colorado.cires.wod.spark.w2p;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;

public class SparklerExecutor {

  private final SparkSession spark;
  private final S3Client s3;
  private final String sourceBucket;
  private final String sourcePrefix;
  private final Path tempDir;
  private final Set<String> sourceFileSubset;
  private final String outputBucket;
  private final String outputPrefix;
  private final Set<String> datasets;
  private final Set<String> processingLevels;
  private final boolean overwrite;
  private final int batchSize;
  private final FileSystemType fs;

  public SparklerExecutor(SparkSession spark, S3Client s3, String sourceBucket, String sourcePrefix, Path tempDir, Set<String> sourceFileSubset,
      String outputBucket, String outputPrefix, Set<String> datasets, Set<String> processingLevels, boolean overwrite, int batchSize,
      FileSystemType fs) {
    this.spark = spark;
    this.s3 = s3;
    this.sourceBucket = sourceBucket;
    this.sourcePrefix = sourcePrefix;
    this.tempDir = tempDir;
    this.sourceFileSubset = sourceFileSubset;
    this.outputBucket = outputBucket;
    this.outputPrefix = outputPrefix;
    this.datasets = datasets;
    this.processingLevels = processingLevels;
    this.overwrite = overwrite;
    this.batchSize = batchSize;
    this.fs = fs;
  }

  public void execute() throws IOException {
    FileActions.mkdir(tempDir);
    List<DatasetTrain> datasetTrains = getDatasetTrains();
    List<DatasetYearTrain> tasks = datasetTrains.stream().flatMap(d -> d.plan().stream()).collect(Collectors.toList());
    tasks.forEach(DatasetYearTrain::run);
  }

  private DatasetTrain createTrain(String dataset, String processingLevel) {
    return new DatasetTrain(spark, s3, dataset, sourceBucket, sourcePrefix, tempDir,
        processingLevel, sourceFileSubset, outputBucket, outputPrefix, overwrite, batchSize, fs);
  }

  private List<DatasetTrain> getDatasetTrains() {
    final int taskCount = datasets.size() * processingLevels.size();
    List<DatasetTrain> datasetTrains = new ArrayList<>(taskCount);
    for (String dataset : datasets) {
      for (String processingLevel : processingLevels) {
        datasetTrains.add(createTrain(dataset, processingLevel));
      }
    }
    return datasetTrains;
  }

}
