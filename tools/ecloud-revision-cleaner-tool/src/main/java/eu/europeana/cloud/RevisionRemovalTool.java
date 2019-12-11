package eu.europeana.cloud;

import eu.europeana.cloud.api.RevisionsReader;
import eu.europeana.cloud.data.RevisionInformation;
import eu.europeana.cloud.jobs.RevisionRemoverJob;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.reader.CSVReader;
import eu.europeana.cloud.utils.CommandLineHelper;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tarek on 7/15/2019.
 */
public class RevisionRemovalTool {
    static final Logger LOGGER = Logger.getLogger(RevisionRemovalTool.class);

    private static final String MCS_URL = "mcs_url";
    private static final String USER_NAME = "username";
    private static final String PASSWORD = "password";
    private static final String REVISION_FILE_PATH = "revisions_file_path";
    private static final String THREADS = "threads";
    private static DataSetServiceClient dataSetServiceClient;
    private static RecordServiceClient recordServiceClient;
    private static RevisionServiceClient revisionServiceClient;
    private static final String DEFAULT_THREADS_NUMBER = "10";
    private static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20000;


    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            LOGGER.info("Starting the cleaning");
            CommandLine cmd = parser.parse(options, args);

            List<RevisionInformation> revisionInformationList = getRevisionInformation(cmd.getOptionValue(REVISION_FILE_PATH));
            initMCSClients(cmd);
            ExecutorService executorService = Executors.newFixedThreadPool(getThreadsNumber(cmd));
            for (RevisionInformation revisionInformation : revisionInformationList) {
                RevisionRemoverJob revisionRemoverJob = new RevisionRemoverJob(dataSetServiceClient, recordServiceClient, revisionInformation, revisionServiceClient);
                executorService.submit(revisionRemoverJob);
            }
            executorService.shutdown();
            executorService.awaitTermination(100, TimeUnit.DAYS);

            LOGGER.info("Finished successfully");
        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Data Cleaner ", options);
            LOGGER.error(exp);
            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("Error while cleaning data " + e.getMessage() + ". Because of " + e.getCause());
            System.exit(1);
        }

    }

    private static List<RevisionInformation> getRevisionInformation(String filePath) throws IOException {
        RevisionsReader revisionsReader = new CSVReader();
        return revisionsReader.getRevisionsInformation(filePath);

    }

    private static void initMCSClients(CommandLine cmd) {
        String mcsUrl = cmd.getOptionValue(MCS_URL);
        String userName = cmd.getOptionValue(USER_NAME);
        String password = cmd.getOptionValue(PASSWORD);
        dataSetServiceClient = new DataSetServiceClient(mcsUrl, null, userName, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, 0);
        recordServiceClient = new RecordServiceClient(mcsUrl, null, userName, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS,0);
        revisionServiceClient = new RevisionServiceClient(mcsUrl, userName, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS,0);

    }

    private static int getThreadsNumber(CommandLine cmd) {
        return Integer.parseInt(cmd.getOptionValue(THREADS, DEFAULT_THREADS_NUMBER));
    }


    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(MCS_URL, "mcs url", true);
        commandLineHelper.addOption(USER_NAME, "user name", true);
        commandLineHelper.addOption(PASSWORD, "password", true);
        commandLineHelper.addOption(REVISION_FILE_PATH, "path to revision information csv file (one revision per line)(String)", true);
        commandLineHelper.addOption(THREADS, "threads (int)", false);

        return commandLineHelper.getOptions();
    }
}
