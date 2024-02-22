package edu.colorado.cires.wod.spark.w2p;

import static edu.colorado.cires.wod.spark.w2p.DatasetTrain.MAX_RECORDS_PER_FILE;

import edu.colorado.cires.wod.ascii.model.Cast;
import java.util.Collections;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class TransformationErrorHandler {

  private final SparkSession spark;
  private final String dataset;
  private final String processingLevel;
  private final String outputBucket;
  private final String outputPrefix;
  private final String key;
  private final String outputParquet;
  private final FileSystemType fs;

  public TransformationErrorHandler(SparkSession spark, String dataset, String processingLevel, String outputBucket, String outputPrefix, String key,
      FileSystemType fs) {
    this.spark = spark;
    this.dataset = dataset;
    this.processingLevel = processingLevel;
    this.outputBucket = outputBucket;
    this.outputPrefix = outputPrefix;
    this.key = key;
    this.fs = fs;
    outputParquet = new StringBuilder(FileSystemPrefix.resolve(fs)).append(this.outputBucket).append("/").append(resolveErrorPrefix()).toString();
  }

  public void handleError(Cast cast, Exception e) {
    String error = ExceptionUtils.getStackTrace(e);
    CastError castError = CastError.builder().withCastNumber(cast.getCastNumber()).withDataset(cast.getDataset()).withError(error).build();
    System.err.println("Conversion error handled: " + cast.getDataset() + ":" + cast.getCastNumber() + "\n" + error);
    spark.createDataset(Collections.singletonList(castError), Encoders.bean(CastError.class))
        .write()
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .format("parquet")
        .mode("append")
        .save(outputParquet);
  }

  private String resolveErrorPrefix() {
    StringBuilder sb = new StringBuilder();
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }
    String[] split = key.split("/");
    String file = split[split.length - 1].replaceAll("\\.gz$", ".parquet");
    sb.append("error/")
        .append(dataset).append("/").append(processingLevel).append("/").append(file);
    return sb.toString();
  }
}
