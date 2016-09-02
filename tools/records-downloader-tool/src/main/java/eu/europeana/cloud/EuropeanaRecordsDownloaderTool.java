package eu.europeana.cloud;

import eu.europeana.cloud.downloader.RecordDownloader;
import eu.europeana.cloud.exception.RepresentationNotFoundException;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.util.CommandLineHelper;
import eu.europeana.cloud.util.FolderCompressor;
import org.apache.commons.cli.*;
import org.apache.tika.mime.MimeTypeException;
import org.zeroturnaround.zip.ZipException;

import java.io.IOException;


/**
 * Created by Tarek on 9/1/2016.
 */
public class EuropeanaRecordsDownloaderTool {

    private final static String MCS_URL = "mcsUrl";
    private final static String USER = "username";
    private final static String PASSWORD = "password";
    private final static String PROVIDER_ID = "provider";
    private final static String DATASET_NAME = "dataset";
    private final static String REPRESENTATION_NAME = "representation";

    public static void main(String[] args) {
        Options options = getRequiredOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String folderPath = executeDownloader(cmd);
            System.out.println("The download completed successfully and the zip folder is located : " + FolderCompressor.compress(folderPath));
        } catch (ParseException exp) {
            System.out.println(exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Migrator", options);
        } catch (RepresentationNotExistsException ex) {
            System.out.println(ex.getMessage());
        } catch (MimeTypeException e) {
            System.out.println("The downloaded records have unrecognised mimeType");
        } catch (ZipException e) {
            System.out.println("Exception happened during zipping the folder" + e.getMessage());
        } catch (MCSException | DriverException e) {
            System.out.println("An exception happened while communicating with mcs" + e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private static Options getRequiredOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addRequiredOption(MCS_URL, "MCS Url");
        commandLineHelper.addRequiredOption(USER, "user");
        commandLineHelper.addRequiredOption(PASSWORD, "password");
        commandLineHelper.addRequiredOption(PROVIDER_ID, "provider id");
        commandLineHelper.addRequiredOption(DATASET_NAME, "dataset name");
        commandLineHelper.addRequiredOption(REPRESENTATION_NAME, "representation name");
        return commandLineHelper.getOptions();
    }

    private static String executeDownloader(CommandLine cmd) throws MimeTypeException, MCSException, RepresentationNotFoundException, IOException {
        String mcsUrl = cmd.getOptionValue(MCS_URL);
        String userName = cmd.getOptionValue(USER);
        String password = cmd.getOptionValue(PASSWORD);
        String providerId = cmd.getOptionValue(PROVIDER_ID);
        String datasetName = cmd.getOptionValue(DATASET_NAME);
        String representation = cmd.getOptionValue(REPRESENTATION_NAME);
        RecordDownloader recordDownloader = new RecordDownloader(new DataSetServiceClient(mcsUrl, userName, password), new FileServiceClient(mcsUrl, userName, password));
        String folderPath = recordDownloader.downloadFilesFromDataSet(providerId, datasetName, representation);
        return folderPath;
    }
}
