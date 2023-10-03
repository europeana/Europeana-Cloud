package eu.europeana.cloud.swiftmigrate;

import eu.europeana.cloud.service.mcs.persistent.s3.SimpleSwiftConnectionProvider;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Migrator {

  public static Logger logger = LoggerFactory.getLogger(Migrator.class);
  private static final String provider = "swift";
  private static CommandLineParser parser;

  private final static String souceContainer = "souceContainer";
  private final static String targetContainer = "targetContainer";
  private final static String endpoint = "endpoint";
  private final static String user = "user";
  private final static String password = "password";


  public static void main(String[] args) {

    CliOptions cliConfig = new CliOptions();
    setCliOptions(cliConfig);

    Options options = cliConfig.getOptions();
    parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException exp) {
      System.out.println("Reason: " + exp.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Migrator", options);
    }
    //obtain parameters
    final String fsourceContainer = cmd.getOptionValue(souceContainer);
    final String ftargetContainer = cmd.getOptionValue(targetContainer);
    final String fendpoint = cmd.getOptionValue(endpoint);
    final String fuser = cmd.getOptionValue(user);
    final String fpassword = cmd.getOptionValue(password);
    //print params
    printCliParamiters(fsourceContainer, ftargetContainer, fendpoint, fuser, fpassword);
    System.out.println("Start migration");
    //start processing
    processFiles(fsourceContainer, fendpoint, fuser, fpassword, ftargetContainer);
    System.out.println("End migration");
  }


  public static void setCliOptions(CliOptions cliConfig)
      throws IllegalArgumentException {
    cliConfig.addCliSetOption(souceContainer);
    cliConfig.addCliSetOption(targetContainer);
    cliConfig.addCliSetOption(endpoint);
    cliConfig.addCliSetOption(user);
    cliConfig.addCliSetOption(password);
  }


  public static void printCliParamiters(final String sourceContainer, final String targetContainer,
      final String endpoint, final String user, final String password) {
    System.out.println("sourceContainer=" + sourceContainer + "\ntargetContainer=" + targetContainer
        + "\nendpoint=" + endpoint + "\nuser=" + user + "\npassword=" + password);
  }


  public static void processFiles(String sourceContainer, String endpoint, String user, String password,
      String targetContainer) {
    final SwiftMigrator migrator = new CustomFileNameMigrator();
    final SimpleSwiftConnectionProvider sourceProvider = new SimpleSwiftConnectionProvider(provider,
        sourceContainer, endpoint, user, password);
    final SimpleSwiftConnectionProvider targetProvider = new SimpleSwiftConnectionProvider(provider,
        targetContainer, endpoint, user, password);
    migrator.chagngeFileName(sourceProvider, targetProvider);
  }
}
