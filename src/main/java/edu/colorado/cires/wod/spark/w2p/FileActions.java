package edu.colorado.cires.wod.spark.w2p;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;

public class FileActions {

  public static void mkdir(Path dir) throws IOException {
    Files.createDirectories(dir);
  }

  public static void mkdirForFile(Path file) throws IOException {
    Path parent = file.getParent();
    if (parent != null) {
      mkdir(parent);
    }
  }

  public static void rm(Path fileOrDir) {
    FileUtils.deleteQuietly(fileOrDir.toFile());
  }

  public static List<String> getPathParts(Path path) {
    Iterator<Path> it = path.iterator();
    List<String> parts = new ArrayList<>();
    while (it.hasNext()) {
      parts.add(it.next().toString());
    }
    return parts;
  }

}
