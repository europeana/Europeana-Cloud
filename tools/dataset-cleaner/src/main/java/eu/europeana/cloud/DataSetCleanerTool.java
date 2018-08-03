package eu.europeana.cloud;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.jobs.VersionRemoverJob;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.utils.CommandLineHelper;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataSetCleanerTool {
    private static final String MCS_URL = "MCS_URL";
    private static final String USERNAME = "USERNAME";
    private static final String PASSWORD = "PASSWORD";
    private static final String DATA_SET_URL = "DATASET_URL";
    private static final String THREADS_COUNT = "THREADS_COUNT";
    private static final int DEFAULT_THREADS_COUNT = 100;


    private static String dataSetUrl;
    private static String mcsURL;
    private static String userName;
    private static String password;
    private static int threadsCount;
    private static String providerId;
    private static String dataSetName;


    private final static Logger LOGGER = Logger.getLogger(DataSetCleanerTool.class);
    public static final int MAXIMUM_FUTURE_NUMBER = 500;

    private static List<String> errorLists = new ArrayList<>();
    private static long successCount = 0;


    public static void main(String[] args) {
        Options options = getParametersHelperOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            setExecusionParameters(cmd);
            UrlParser urlParser = new UrlParser(dataSetUrl);
            if (urlParser.isUrlToDataset()) {
                providerId = urlParser.getPart(UrlPart.DATA_PROVIDERS);
                dataSetName = urlParser.getPart(UrlPart.DATA_SETS);
                removeFilesFromDataSet();
            } else
                LOGGER.error("The provided dataSet url is not formulated correctly");

        } catch (ParseException exp) {
            LOGGER.error(exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Please provide those parameters to the tool:", options);

        } catch (Exception e) {
            LOGGER.error("An exception happened caused by: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void setExecusionParameters(CommandLine cmd) {
        dataSetUrl = cmd.getOptionValue(DATA_SET_URL);
        mcsURL = cmd.getOptionValue(MCS_URL);
        userName = cmd.getOptionValue(USERNAME);
        password = cmd.getOptionValue(PASSWORD);
        threadsCount = DEFAULT_THREADS_COUNT;
        if (cmd.getOptionValue(THREADS_COUNT) != null)
            threadsCount = Integer.parseInt(cmd.getOptionValue(THREADS_COUNT));
    }

    private static void removeFilesFromDataSet() {
        ExecutorService service = Executors.newFixedThreadPool(threadsCount);

        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(mcsURL, userName, password);
        RepresentationIterator representationIterator = dataSetServiceClient.getRepresentationIterator(providerId, dataSetName);
        int threadsInWorkCount = 0;
        Set<Future<String>> futures = new HashSet<>(MAXIMUM_FUTURE_NUMBER);

        while (representationIterator.hasNext()) {
            Representation representation = representationIterator.next();
            Future<String> future = service.submit(new VersionRemoverJob(mcsURL, representation, userName, password));
            futures.add(future);
            threadsInWorkCount++;
            if (threadsInWorkCount == MAXIMUM_FUTURE_NUMBER) {
                getExcisionResultAndWait(futures);
                viewReport();
                threadsInWorkCount = 0;
            }
        }

        if (!futures.isEmpty())
            getExcisionResultAndWait(futures);
        viewReport();
        LOGGER.info("The tool ended its Job");
        service.shutdown();
    }

    private static void getExcisionResultAndWait(Set<Future<String>> futures) {
        int errorsCountInThisBatch = 0;
        for (Future<String> futureItem : futures) {
            try {
                System.out.println(futureItem.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                errorsCountInThisBatch++;
                errorLists.add(e.getMessage());
            }
        }
        successCount += (MAXIMUM_FUTURE_NUMBER - errorsCountInThisBatch);
        futures.clear();
    }

    private static void viewReport() {
        LOGGER.info("You correctly Removed " + successCount + " From data set and encountered " + errorLists.size() + " errors");
        if (!errorLists.isEmpty()) {
            LOGGER.info("The detailed error report till now is: ");
            LOGGER.info("***********");
            for (String errorMessage : errorLists) {
                LOGGER.error(errorMessage);
            }
            LOGGER.info("***********");
        }
    }


    private static Options getParametersHelperOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(MCS_URL, "URL for mcs", true);
        commandLineHelper.addOption(USERNAME, "user name", true);
        commandLineHelper.addOption(PASSWORD, "password", true);
        commandLineHelper.addOption(DATA_SET_URL, "data set URL", true);
        commandLineHelper.addOption(THREADS_COUNT, "threads count (int)(optional)(default=10)", false);
        return commandLineHelper.getOptions();
    }

}
