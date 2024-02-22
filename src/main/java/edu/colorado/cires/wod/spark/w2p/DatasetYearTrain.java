package edu.colorado.cires.wod.spark.w2p;

import static edu.colorado.cires.wod.spark.w2p.DatasetTrain.MAX_RECORDS_PER_FILE;
import static edu.colorado.cires.wod.spark.w2p.FileActions.mkdirForFile;
import static edu.colorado.cires.wod.spark.w2p.FileActions.rm;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.deletePrefix;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.download;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.exists;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.listObjects;

import edu.colorado.cires.wod.ascii.reader.BufferedCharReader;
import edu.colorado.cires.wod.ascii.reader.CastFileReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.transformer.ascii2parquet.WodAsciiParquetTransformer;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;

public class DatasetYearTrain implements Callable<String> {

  private final int batchSize;
  private static final int GEOHASH_LENGTH = 3;

  private final DatasetTrain datasetTrain;
  private final SparkSession spark;
  private final S3Client s3;
  private final String dataset;
  private final String sourceBucket;
  private final Path tempDir;
  private final String processingLevel;
  private final String outputBucket;
  private final String outputPrefix;
  private final String key;
  private final String outputParquet;
  private final boolean overwrite;
  private final String keyPrefix;
  private final TransformationErrorHandler transformationErrorHandler;
  private final FileSystemType fs;

  public DatasetYearTrain(DatasetTrain datasetTrain, SparkSession spark, S3Client s3, String dataset, String sourceBucket, Path tempDir,
      String processingLevel, String outputBucket,
      String outputPrefix, String key, boolean overwrite, int batchSize,
      TransformationErrorHandler transformationErrorHandler, FileSystemType fs) {
    this.batchSize = batchSize;
    this.datasetTrain = datasetTrain;
    this.spark = spark;
    this.s3 = s3;
    this.dataset = dataset;
    this.sourceBucket = sourceBucket;
    this.tempDir = tempDir;
    this.processingLevel = processingLevel;
    this.outputBucket = outputBucket;
    this.outputPrefix = outputPrefix;
    this.key = key;
    this.overwrite = overwrite;
    this.transformationErrorHandler = transformationErrorHandler;
    this.fs = fs;
    keyPrefix = resolveKeyPrefix();
    outputParquet = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/").append(keyPrefix).toString();
  }

  public DatasetTrain getDatasetTrain() {
    return datasetTrain;
  }

  public String getOutputParquet() {
    return outputParquet;
  }

  @Override
  public String call() throws Exception {
    try {
      if (overwrite || !listObjects(s3, outputBucket, keyPrefix + "/_temporary/", x -> true).isEmpty()) {
        deletePrefix(s3, outputBucket, keyPrefix + "/");
      }
        if (exists(s3, outputBucket, keyPrefix + "/_SUCCESS")) {
          System.err.println("Skipping existing " + outputParquet);
        } else {
          System.err.println("Free space: " + (tempDir.toFile().getFreeSpace() / 1024 / 1024) + "MiB");
          System.err.println("Downloading s3://" + sourceBucket + "/" + key);
          Path file = tempDir.resolve(key);
          System.err.println("Done downloading s3://" + sourceBucket + "/" + key);
          mkdirForFile(file);
          try {
            download(s3, sourceBucket, key, file);
            processFile(file, outputParquet);
          } finally {
            rm(file);
          }
        }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to process file " + key, e);
    }
    return outputParquet;
  }

  private void processFile(Path file, String outputParquet) throws IOException {
    try (InputStream inputStream = Files.newInputStream(file)) {
      processFile(inputStream, outputParquet);
    }
  }

  private void processFile(InputStream inputStream, String outputParquet) throws IOException {

    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(inputStream)), StandardCharsets.UTF_8))) {

      CastFileReader reader = new CastFileReader(new BufferedCharReader(bufferedReader), dataset);

      final BlockingQueue<CastWrapper> transferQueue = new LinkedBlockingDeque<>(batchSize);
      final Thread writer = new Thread(() -> {
        CastWrapper castWrapper;
        try {
          castWrapper = transferQueue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        List<Cast> batch = new ArrayList<>(batchSize);
        boolean first = true;
        while (!castWrapper.isPoison()) {
          if (batch.size() < batchSize) {
            batch.add(castWrapper.getCast());
          }
          if (batch.size() == batchSize) {
            writeBatch(batch, outputParquet, first);
            first = false;
            batch = new ArrayList<>(batchSize);
          }
          try {
            castWrapper = transferQueue.take();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        if (!batch.isEmpty()) {
          writeBatch(batch, outputParquet, first);
        }
      });
      writer.start();

      try {
        while (reader.hasNext()) {
          edu.colorado.cires.wod.ascii.model.Cast asciiCast = reader.next();
          try {
            Cast cast = WodAsciiParquetTransformer.parquetFromAscii(asciiCast, GEOHASH_LENGTH);
            transferQueue.put(new CastWrapper(cast));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } catch (Exception e) {
            transformationErrorHandler.handleError(asciiCast, e);
          }
        }
      } finally {
        try {
          transferQueue.put(new CastWrapper(null));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      try {
        writer.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private void writeBatch(List<Cast> batch, String outputParquet, boolean isNew) {
    String mode = isNew ? "overwrite" : "append";
    System.err.println("Submitted batch write: " + batch.size() + " " + mode);
    spark.createDataset(batch, Encoders.bean(Cast.class))
        .write()
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .format("parquet")
        .mode(mode)
        .partitionBy("geohash", "year")
        .save(outputParquet);
  }

  private String resolveKeyPrefix() {
    StringBuilder sb = new StringBuilder();
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }
    String[] split = key.split("/");
    String file = split[split.length - 1].replaceAll("\\.gz$", ".parquet");
    sb.append("yearly/")
        .append(dataset).append("/").append(processingLevel).append("/").append(file);
    return sb.toString();
  }

  private static class CastWrapper {

    private final Cast cast;

    public CastWrapper(Cast cast) {
      this.cast = cast;
    }

    public Cast getCast() {
      return cast;
    }

    public boolean isPoison() {
      return cast == null;
    }

  }

}
