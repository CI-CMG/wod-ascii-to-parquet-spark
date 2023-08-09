package edu.colorado.cires.wod.spark.w2p;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Actions {

  public static InputStream openDownloadStream(S3Client s3, String bucket, String key) throws IOException {
    return new BufferedInputStream(s3.getObject(c -> c.bucket(bucket).key(key)));
  }

  public static void download(S3Client s3, String bucket, String key, Path file) throws IOException {
    try (InputStream in = openDownloadStream(s3, bucket, key);
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(file));
    ) {
      IOUtils.copy(in, out);
    }
  }

  public static Set<String> listObjects(S3Client s3, String bucket, String keyPrefix, Predicate<String> filter) {
    Set<String> keys = new TreeSet<>();
    for (ListObjectsV2Response page : s3.listObjectsV2Paginator(c -> c.bucket(bucket).prefix(keyPrefix))) {
      keys.addAll(page.contents().stream().map(S3Object::key).filter(filter).collect(Collectors.toList()));
    }
    return keys;
  }

  private static final int MAX_DELETE_COUNT = 1000;

  public static void deletePrefix(S3Client s3, String bucket, String keyPrefix) {
    System.err.println("Deleting (if exists) s3://" + bucket + "/" + keyPrefix + "*");
    List<ObjectIdentifier> ois = listObjects(s3, bucket, keyPrefix, x -> true).stream()
        .map(key -> ObjectIdentifier.builder().key(key).build())
        .collect(Collectors.toList());
    if (!ois.isEmpty()) {
      List<List<ObjectIdentifier>> chunks = IntStream.iterate(0, i -> i < ois.size(), i -> i + MAX_DELETE_COUNT)
          .mapToObj(i -> ois.subList(i, Math.min(i + MAX_DELETE_COUNT, ois.size())))
          .collect(Collectors.toList());
      chunks.stream()
          .map(list -> list.toArray(new ObjectIdentifier[0]))
          .map(array -> Delete.builder().objects(array).build())
          .map(delete -> DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build())
          .forEach(s3::deleteObjects);
    }
  }

  public static boolean exists(S3Client s3, String bucket, String key) {
    try {
      s3.headObject(c -> c.bucket(bucket).key(key));
    } catch (NoSuchKeyException e) {
      return false;
    }
    return true;
  }
}
