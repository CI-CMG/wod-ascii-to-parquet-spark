package edu.colorado.cires.wod.spark.w2p;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
  private final int concurrency;
  private final boolean overwrite;
  private final int batchSize;
  private final boolean emr;

  public SparklerExecutor(SparkSession spark, S3Client s3, String sourceBucket, String sourcePrefix, Path tempDir, Set<String> sourceFileSubset,
      String outputBucket, String outputPrefix, Set<String> datasets, Set<String> processingLevels, int concurrency, boolean overwrite, int batchSize,
      boolean emr) {
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
    this.concurrency = concurrency;
    this.overwrite = overwrite;
    this.batchSize = batchSize;
    this.emr = emr;
  }

  public void execute() throws IOException {
    FileActions.mkdir(tempDir);
    List<DatasetTrain> datasetTrains = getDatasetTrains();
    List<DatasetYearTrain> tasks = datasetTrains.stream().flatMap(d -> d.plan().stream()).collect(Collectors.toList());
    Map<DatasetTrain, Set<String>> accumulator = new HashMap<>();
    Map<String, DatasetTrain> keyMap = new HashMap<>();
    for (DatasetYearTrain task : tasks) {
      DatasetTrain datasetTrain = task.getDatasetTrain();
      Set<String> keySet = accumulator.get(datasetTrain);
      if (keySet == null) {
        keySet = new HashSet<>();
        accumulator.put(datasetTrain, keySet);
      }
      String key = task.getOutputParquet();
      keySet.add(key);
      keyMap.put(key, datasetTrain);
    }

    Map<DatasetTrain, Set<String>> todoMap = new HashMap<>();

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      List<Future<String>> results = tasks.stream().map(executor::submit).collect(Collectors.toList());
      for (Future<String> result : results) {
        String key = result.get();
        DatasetTrain datasetTrain = keyMap.get(key);
        Set<String> remaining = accumulator.get(datasetTrain);
        remaining.remove(key);
        Set<String> todo = todoMap.get(datasetTrain);
        if (todo == null) {
          todo = new TreeSet<>();
          todoMap.put(datasetTrain, todo);
        }
        todo.add(key);
        if (remaining.isEmpty()) {
          datasetTrain.accumulate(new ArrayList<>(todo));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Task exection was interrupted", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("An error occurred while processing task", e);
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
  }

  private DatasetTrain createTrain(String dataset, String processingLevel) {
    return new DatasetTrain(spark, s3, dataset, sourceBucket, sourcePrefix, tempDir,
        processingLevel, sourceFileSubset, outputBucket, outputPrefix, overwrite, batchSize, emr);
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
