package edu.colorado.cires.wod.spark.w2p;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(
    name = "s3-list-missing",
    description = "Compares ASCII files in bucket",
    mixinStandardHelpOptions = true)
public class OsPoolS3Lister implements Runnable {

  @Option(names = {"-b", "--bucket-path"}, required = true, description = "The s3 url to scan")
  private String bucketPath;
  @Option(names = {"-l", "--list-file"}, required = true, description = "The list file")
  private Path listFile;
  @Option(names = {"-o", "--output-file"}, required = true, description = "The list file")
  private Path outputFile;

  private static String normalize(String key) {
    if (StringUtils.isBlank(key) || key.equals("/")) {
      return "";
    }
    return key.replaceAll("/+$", "") + "/";
  }

  private static String getFileName(String key) {
    String[] split = key.split("/");
    return split[split.length - 1];
  }

  private static DatasetYear parseDatasetYear(String key) {
    if (key.contains("SUR")) {
      return new DatasetYear("SUR", "ALL");
    }
    return new DatasetYear(key.substring(0, 3), key.substring(4, 8));
  }

  private Set<DatasetYear> getAll()  {
    try {
      List<String> lines = Files.readAllLines(listFile, StandardCharsets.UTF_8);
      return new TreeSet<>(lines.stream()
          .filter(StringUtils::isNotBlank)
          .map(StringUtils::trim)
          .map(line -> {
            String[] split = line.split(",");
            return new DatasetYear(split[1], split[0]);
          })
          .collect(Collectors.toSet()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run() {

    AmazonS3URI s3Uri = new AmazonS3URI(bucketPath);
    String targetBucket = s3Uri.getBucket();
    String targetKey = normalize(s3Uri.getKey());
    AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    ListObjectsV2Request listObjectsReqManual = new ListObjectsV2Request()
        .withBucketName(targetBucket)
        .withPrefix(targetKey)
        .withMaxKeys(100);

    Set<DatasetYear> all = getAll();
    Set<DatasetYear> success = new TreeSet<>();

    boolean done = false;
    while (!done) {
      ListObjectsV2Result listObjResponse = s3.listObjectsV2(listObjectsReqManual);
      success.addAll(listObjResponse.getObjectSummaries().stream()
          .map(S3ObjectSummary::getKey)
          .map(OsPoolS3Lister::getFileName)
          .filter(key -> key.endsWith(".gz"))
          .map(OsPoolS3Lister::parseDatasetYear)
          .collect(Collectors.toList()));

      if (listObjResponse.getNextContinuationToken() == null) {
        done = true;
      }

      listObjectsReqManual = listObjectsReqManual.withContinuationToken(listObjResponse.getNextContinuationToken());
    }

    all.removeAll(success);

    Path parent = outputFile.getParent();
    if (parent != null) {
      try {
        Files.createDirectories(parent);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    try (OutputStream outputStream = Files.newOutputStream(outputFile)) {
      for (DatasetYear missing : all) {
        outputStream.write(missing.toLine().getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private static class DatasetYear implements Comparable<DatasetYear> {
    private final String dataset;
    private final String year;

    private DatasetYear(String dataset, String year) {
      this.dataset = dataset;
      this.year = year;
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DatasetYear that = (DatasetYear) o;
      return Objects.equals(year, that.year) && Objects.equals(dataset, that.dataset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(year, dataset);
    }

    @Override
    public String toString() {
      return "DatasetYear{" +
          "dataset='" + dataset + '\'' +
          ", year='" + year + '\'' +
          '}';
    }

    public String toLine() {
      return year + "," + dataset + "\n";
    }

    @Override
    public int compareTo(@NotNull OsPoolS3Lister.DatasetYear o) {
      return toString().compareTo(o.toString());
    }
  }
}
