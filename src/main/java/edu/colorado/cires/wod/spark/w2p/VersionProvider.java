package edu.colorado.cires.wod.spark.w2p;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import picocli.CommandLine.IVersionProvider;

public class VersionProvider implements IVersionProvider {

  private static String version;

  @Override
  public String[] getVersion() throws Exception {
    if (version == null) {
      try (BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("version.txt"), StandardCharsets.UTF_8))) {
        version = in.readLine();
      }
    }
    return new String[]{version};
  }

}
