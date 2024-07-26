package edu.colorado.cires.wod.spark.w2p;

import static edu.colorado.cires.wod.spark.w2p.FileActions.mkdirForFile;
import static edu.colorado.cires.wod.spark.w2p.FileActions.rm;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.deletePrefix;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.download;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.exists;
import static edu.colorado.cires.wod.spark.w2p.S3Actions.listObjects;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;

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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;

public class DatasetYearTrain implements Runnable {

  private static final String WCS84_PROJJSON = "{\"$schema\": \"https://proj.org/schemas/v0.7/projjson.schema.json\",\"type\": \"GeographicCRS\",\"name\": \"WGS 84\",\"datum_ensemble\": {\"name\": \"World Geodetic System 1984 ensemble\",\"members\": [{\"name\": \"World Geodetic System 1984 (Transit)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1166}},{\"name\": \"World Geodetic System 1984 (G730)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1152}},{\"name\": \"World Geodetic System 1984 (G873)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1153}},{\"name\": \"World Geodetic System 1984 (G1150)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1154}},{\"name\": \"World Geodetic System 1984 (G1674)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1155}},{\"name\": \"World Geodetic System 1984 (G1762)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1156}},{\"name\": \"World Geodetic System 1984 (G2139)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1309}}],\"ellipsoid\": {\"name\": \"WGS 84\",\"semi_major_axis\": 6378137,\"inverse_flattening\": 298.257223563},\"accuracy\": \"2.0\",\"id\": {\"authority\": \"EPSG\",\"code\": 6326}},\"coordinate_system\": {\"subtype\": \"ellipsoidal\",\"axis\": [{\"name\": \"Geodetic latitude\",\"abbreviation\": \"Lat\",\"direction\": \"north\",\"unit\": \"degree\"},{\"name\": \"Geodetic longitude\",\"abbreviation\": \"Lon\",\"direction\": \"east\",\"unit\": \"degree\"}]},\"scope\": \"Horizontal component of 3D system.\",\"area\": \"World.\",\"bbox\": {\"south_latitude\": -90,\"west_longitude\": -180,\"north_latitude\": 90,\"east_longitude\": 180},\"id\": {\"authority\": \"EPSG\",\"code\": 4326}}";
  private static final String GEOPARQUET_VERSION = "1.0.0";

  private final int batchSize;

  private final SparkSession spark;
  private final S3Client s3;
  private final String dataset;
  private final String sourceBucket;
  private final Path tempDir;
  private final String processingLevel;
  private final String outputBucket;
  private final String outputPrefix;
  private final String key;
  private final String preProcessingOutputParquet;
  private final String finalOutputParquet;
  private final boolean overwrite;
  private final String keyPrefix;
  private final TransformationErrorHandler transformationErrorHandler;
  private final FileSystemType ifs;
  private final FileSystemType ofs;

  public DatasetYearTrain(SparkSession spark, S3Client s3, String dataset, String sourceBucket, Path tempDir,
      String processingLevel, String outputBucket,
      String outputPrefix, String key, boolean overwrite, int batchSize,
      TransformationErrorHandler transformationErrorHandler, FileSystemType ifs, FileSystemType ofs) {
    this.batchSize = batchSize;
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
    this.ifs = ifs;
    this.ofs = ofs;
    keyPrefix = resolveKeyPrefix();
    finalOutputParquet = new StringBuilder(FileSystemPrefix.resolve(ifs)).append(outputBucket).append("/").append(keyPrefix).toString();
    preProcessingOutputParquet = finalOutputParquet + "_temp";
  }

  @Override
  public void run() {
    try {
      if (overwrite || !listObjects(ofs, s3, outputBucket, keyPrefix + "/_temporary/", x -> true).isEmpty()) {
        deletePrefix(ofs, s3, outputBucket, keyPrefix + "/");
      }
      if (exists(ofs, s3, outputBucket, keyPrefix + "/_SUCCESS")) {
        System.err.println("Skipping existing " + preProcessingOutputParquet);
      } else {
        if (ifs == FileSystemType.local) {
          Path file = Paths.get(sourceBucket).resolve(key);
          processFile(file);
        } else {
          System.err.println("Free space: " + (tempDir.toFile().getFreeSpace() / 1024 / 1024) + "MiB");
          System.err.println("Downloading s3://" + sourceBucket + "/" + key);
          Path file = tempDir.resolve(key);
          mkdirForFile(file);
          try {
            download(ifs, s3, sourceBucket, key, file);
            System.err.println("Done downloading s3://" + sourceBucket + "/" + key);
            processFile(file);
          } finally {
            rm(file);
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to process file " + key, e);
    }
  }

  private void processFile(Path file) throws IOException {
    try (InputStream inputStream = Files.newInputStream(file)) {
      processFile(inputStream);
    }
    finalizeAndSort();
  }

  private void deletePreprocessing() {
    deletePrefix(ofs, s3, outputBucket, keyPrefix + "_temp/");
  }

  private void finalizeAndSort() {
    System.err.println("Sorting and writing final output: " + finalOutputParquet);
    Dataset<Row> dataset = spark.read().format("geoparquet").load(preProcessingOutputParquet).orderBy(asc("geohash"));
    dataset.repartition(col("geohash3")).sortWithinPartitions("geohash").write()
        .format("geoparquet")
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .mode(SaveMode.Overwrite)
        .partitionBy("geohash3")
        .save(finalOutputParquet);
    deletePreprocessing();
  }

  private void processFile(InputStream inputStream) throws IOException {

    try (BufferedReader bufferedReader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new BufferedInputStream(inputStream)), StandardCharsets.UTF_8))) {

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
            writeBatch(batch, first);
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
          writeBatch(batch, first);
        }
      });
      writer.start();

      try {
        while (reader.hasNext()) {
          edu.colorado.cires.wod.ascii.model.Cast asciiCast = reader.next();
          try {
            Cast cast = WodAsciiParquetTransformer.parquetFromAscii(asciiCast);
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

  private void writeBatch(List<Cast> batch, boolean isNew) {
    String mode = isNew ? "overwrite" : "append";
    System.err.println("Submitted batch write: " + batch.size() + " " + mode);
    spark.createDataset(batch, Encoders.bean(Cast.class))
        .write()
        .format("geoparquet")
//        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .mode(mode)
        .save(preProcessingOutputParquet);
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
