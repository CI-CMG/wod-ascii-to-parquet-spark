package edu.colorado.cires.wod.spark.w2p;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "os-pool-utils",
    description = "Command line utilities for running jobs in OSPool",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {
        OsPoolHttpDownloader.class,
        OsPoolS3Uploader.class,
        OsPoolS3Lister.class
    }
)
public class OsPoolUtils implements Runnable {

  @Spec
  private CommandSpec spec;


  public static void main(String[] args) {
    System.exit(new CommandLine(new OsPoolUtils()).execute(args));
  }

  public void run() {
    spec.commandLine().usage(System.out);
  }
}
