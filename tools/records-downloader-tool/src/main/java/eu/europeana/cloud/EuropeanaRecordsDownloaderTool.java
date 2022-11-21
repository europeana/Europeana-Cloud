package eu.europeana.cloud;

import eu.europeana.cloud.downloader.RecordDownloader;
import eu.europeana.cloud.exception.RepresentationNotFoundException;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.util.CommandLineHelper;
import eu.europeana.cloud.util.FileUtil;
import eu.europeana.cloud.util.FolderCompressor;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.tika.mime.MimeTypeException;
import org.zeroturnaround.zip.ZipException;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;


/**
 * Created by Tarek on 9/1/2016.
 */
public class EuropeanaRecordsDownloaderTool {

  private static final String MCS_URL = "mcsUrl";
  private static final String USER = "username";
  private static final String PASSWORD = "password";
  private static final String PROVIDER_ID = "provider";
  private static final String DATASET_NAME = "dataset";
  private static final String REPRESENTATION_NAME = "representation";
  private static final String THREADS_COUNT = "threads";
  private static final int DEFAULT_THREADS_COUNT = 10;

  public static void main(String[] args) {
    Options options = getOptions();
    CommandLineParser parser = new DefaultParser();
    String zipFolderPath = FileUtil.createZipFolderPath(new Date());
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      String folderPath = executeDownloader(cmd);
      FolderCompressor.compress(folderPath, zipFolderPath);
      System.out.println("The download completed successfully and the zip folder is located : " + zipFolderPath);
    } catch (ParseException exp) {
      System.out.println(exp.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Records downloader ", options);
    } catch (RepresentationNotFoundException ex) {
      System.out.println(ex.getMessage());
    } catch (MimeTypeException e) {
      System.out.println("The downloaded records have unrecognised mimeType");
    } catch (ZipException e) {
      System.out.println("Exception happened during zipping the folder " + e.getMessage());
      try {
        if (zipFolderPath != null) {
          FileUtils.forceDelete(new File(zipFolderPath));
        }
      } catch (IOException io) {
        System.out.println("An Exception happened while deleting folder " + zipFolderPath);
      }
    } catch (NumberFormatException e) {
      System.out.println("Threads count should be integer");
    } catch (DriverException e) {
      System.out.println(
          "An exception happened during communicating with MCS: " + cmd.getOptionValue(MCS_URL) + " caused by " + e.getMessage());
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("An exception happened during downloading the files caused by: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("An exception happened caused by: " + e.getMessage());
    }

  }

  private static Options getOptions() {
    CommandLineHelper commandLineHelper = new CommandLineHelper();
    commandLineHelper.addOption(MCS_URL, "MCS Url", true);
    commandLineHelper.addOption(USER, "user", true);
    commandLineHelper.addOption(PASSWORD, "password", true);
    commandLineHelper.addOption(PROVIDER_ID, "provider id", true);
    commandLineHelper.addOption(DATASET_NAME, "dataset name", true);
    commandLineHelper.addOption(REPRESENTATION_NAME, "representation name", true);
    commandLineHelper.addOption(THREADS_COUNT, "threads count (int)(optional)(default=10)", false);
    return commandLineHelper.getOptions();
  }

  private static String executeDownloader(CommandLine cmd)
      throws InterruptedException, ExecutionException, MimeTypeException, RepresentationNotFoundException, IOException {
    String mcsUrl = cmd.getOptionValue(MCS_URL);
    String userName = cmd.getOptionValue(USER);
    String password = cmd.getOptionValue(PASSWORD);
    String providerId = cmd.getOptionValue(PROVIDER_ID);
    String datasetName = cmd.getOptionValue(DATASET_NAME);
    String representation = cmd.getOptionValue(REPRESENTATION_NAME);
    String threads = cmd.getOptionValue(THREADS_COUNT);
    int threadsCount = DEFAULT_THREADS_COUNT;
    if (threads != null) {
      threadsCount = Integer.parseInt(threads);
    }
    RecordDownloader recordDownloader = new RecordDownloader(new DataSetServiceClient(mcsUrl, userName, password),
        new FileServiceClient(mcsUrl, userName, password));
    return recordDownloader.downloadFilesFromDataSet(providerId, datasetName, representation, threadsCount);

  }
}
