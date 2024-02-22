package edu.colorado.cires.wod.spark.w2p;

import edu.colorado.cires.cmg.s3out.MultipartUploadRequest;
import edu.colorado.cires.cmg.s3out.S3ClientMultipartUpload;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Actions {

  public static InputStream openDownloadStream(FileSystemType fs, S3Client s3, String bucket, String key) throws IOException {
    if (fs == FileSystemType.local) {
      Path path;
      if (key == null) {
        path = Paths.get(bucket);
      } else {
        path = Paths.get(bucket).resolve(key);
      }
      return new BufferedInputStream(Files.newInputStream(path));
    } else {
      return new BufferedInputStream(s3.getObject(c -> c.bucket(bucket).key(key)));
    }
  }

  public static void download(FileSystemType fs, S3Client s3, String bucket, String key, Path file) throws IOException {
    try (InputStream in = openDownloadStream(fs, s3, bucket, key);
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(file));
    ) {
      IOUtils.copy(in, out);
    }
  }



  public static Set<String> listObjects(FileSystemType fs, S3Client s3, String bucket, String keyPrefix, Predicate<String> filter) {
    if (fs == FileSystemType.local) {
      return listFiles(bucket, keyPrefix, filter);
    } else {
      Set<String> keys = new TreeSet<>();
      for (ListObjectsV2Response page : s3.listObjectsV2Paginator(c -> c.bucket(bucket).prefix(keyPrefix))) {
        keys.addAll(page.contents().stream().map(S3Object::key).filter(filter).collect(Collectors.toList()));
      }
      return keys;
    }
  }

  private static Set<String> listFiles(String bucket, String keyPrefix, Predicate<String> filter) {
    Path bucketPath = Paths.get(bucket).toAbsolutePath().normalize();
    Set<String> keys = new TreeSet<>();
    Path path;
    if (keyPrefix == null) {
      path = bucketPath;
    } else {
      path = bucketPath.resolve(keyPrefix);
    }
    if (!Files.isDirectory(path)) {
      return keys;
    }
    try (Stream<Path> stream = Files.walk(path.toAbsolutePath().normalize())) {
      keys.addAll(stream.filter(Files::isRegularFile).map(p -> p.toAbsolutePath().normalize()).map(bucketPath::relativize).map(Path::toString).filter(filter).collect(Collectors.toList()));
    } catch (IOException e) {
      throw new RuntimeException("Unable to list files", e);
    }
    return keys;
  }

  private static final int MAX_DELETE_COUNT = 1000;

  public static void deletePrefix(FileSystemType fs, S3Client s3, String bucket, String keyPrefix) {
    if (fs == FileSystemType.local) {
      Path path;
      if (keyPrefix == null) {
        path = Paths.get(bucket);
      } else {
        path = Paths.get(bucket).resolve(keyPrefix);
      }
      FileUtils.deleteQuietly(path.toFile());
    } else {
      System.err.println("Deleting (if exists) s3://" + bucket + "/" + keyPrefix + "*");
      List<ObjectIdentifier> ois = listObjects(fs, s3, bucket, keyPrefix, x -> true).stream()
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
  }

  public static boolean exists(FileSystemType fs, S3Client s3, String bucket, String key) {
    if (fs == FileSystemType.local) {
      Path path;
      if (key == null) {
        path = Paths.get(bucket);
      } else {
        path = Paths.get(bucket).resolve(key);
      }
      return Files.isRegularFile(path);
    } else {
      try {
        s3.headObject(c -> c.bucket(bucket).key(key));
      } catch (NoSuchKeyException e) {
        return false;
      }
      return true;
    }
  }
}
