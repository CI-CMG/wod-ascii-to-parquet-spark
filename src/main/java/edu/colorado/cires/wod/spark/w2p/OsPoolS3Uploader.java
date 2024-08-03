package edu.colorado.cires.wod.spark.w2p;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(
    name = "s3-upload",
    description = "Upload a directory to a S3 bucket",
    mixinStandardHelpOptions = true)
public class OsPoolS3Uploader implements Runnable {

  @Option(names = {"-b", "--bucket-path"}, required = true, description = "The URL to download")
  private String bucketPath;
  @Option(names = {"-d", "--dir"}, required = true, description = "The file to download to")
  private Path source;

  public static String normalize(String key) {
    if (StringUtils.isBlank(key) || key.equals("/")) {
      return "";
    }
    return key.replaceAll("/+$", "");
  }

  private void transfer(Transfer transfer) {
    do {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    } while (!transfer.isDone());
    TransferState state = transfer.getState();
    System.out.println(": " + state);
    if (state == TransferState.Failed || state == TransferState.Canceled) {
      try {
        transfer.waitForCompletion();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Transfer Failed", e);
      }
    }
  }

  public void upload(Path source, String targetBucket, String targetKey, AmazonS3 s3) {
    TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
    Upload upload = transferManager.upload(targetBucket, targetKey, source.toFile());
    transfer(upload);
    transferManager.shutdownNow(false);
  }

  @Override
  public void run() {

    AmazonS3URI s3Uri = new AmazonS3URI(bucketPath);
    String targetBucket = s3Uri.getBucket();
    String targetKey = normalize(s3Uri.getKey());
    AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    try (Stream<Path> paths = Files.walk(source).filter(f -> !Files.isDirectory(f))) {
      paths.forEach(path -> {
        Path tail = source.relativize(path);
        String tk = targetKey.isEmpty() ? tail.toString() : targetKey + "/" + tail.toString();
        upload(path, targetBucket, tk, s3);
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
