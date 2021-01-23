package eu.europeana.cloud;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.persisted.ProviderRemover;
import eu.europeana.cloud.utils.CommandLineHelper;
import eu.europeana.cloud.utils.Toolkit;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProviderCleanerTool {
    static final Logger LOGGER = Logger.getLogger(ProviderCleanerTool.class);

    private static final String URL = "url";
    private static final String TEST_MODE = "test-mode";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String PROVIDER_ID = "provider-id";
    private static final String PROVIDERS_FILE = "providers-file";
    private static final String ONLY_DATASETS = "only-datasets";
    private static final String ONLY_RECORDS = "only-records";
    private static final String RECORDS_FILE = "records-file";

    /**
     * Exmples using test/acceptance database and server for given one
     *
     * Example parameters for cleaning only records in test mode
     * --test-mode yes --only-records yes --url https://test.ecloud.psnc.pl/api --username admin --password ***** --providers-file /path/to/providers/file/test_providers
     *
     * Example parameters for cleaning only datasets in normal mode
     * --only-datasets yes --url https://test.ecloud.psnc.pl/api --username metis_test --password ***** --providers-file /path/to/providers/file/test_providers
     *
     * Passwords check in ecloud_aas_v1.users table
     * @param args
     */
    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            LOGGER.info("Starting the cleaning records and datasets for providers");
            CommandLine cmd = parser.parse(options, args);
            remove(cmd);
            LOGGER.info("Finished successfully");
            System.exit(0);
        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Data Cleaner ", options);
        } catch (Exception e) {
            LOGGER.error(String.format("Error while cleaning data %s. Because of %n", e.getMessage()), e);
            System.exit(1);
        }
    }

    private static void remove(CommandLine cmd) throws CloudException, IOException {
        List<String> providers = null;
        String recordsFile = null;

        if(cmd.hasOption(PROVIDERS_FILE) || cmd.hasOption(PROVIDER_ID)) {
            String providersFilename = cmd.getOptionValue(PROVIDERS_FILE, null);
            if (providersFilename != null) {
                providers = Toolkit.readIdentifiers(providersFilename);
            } else {
                String singleProvider = cmd.getOptionValue(PROVIDER_ID, null);
                if (singleProvider != null) {
                    providers = new ArrayList<>();
                    providers.add(singleProvider);
                } else {
                    LOGGER.error("At least one provider id must be provided (nomen omen) to this tool program");
                    System.exit(-1);
                }
            }
        }
        if(cmd.hasOption(RECORDS_FILE)) {
            recordsFile = cmd.getOptionValue(RECORDS_FILE, null);
        }

        ProviderRemover providerRemover = new ProviderRemover(
                cmd.getOptionValue(URL),
                cmd.getOptionValue(USERNAME),
                cmd.getOptionValue(PASSWORD),
                cmd.hasOption(TEST_MODE)
        );

        if(!cmd.hasOption(ONLY_DATASETS)) {
            if(recordsFile != null) {
                providerRemover.removeRecordsFromFile(recordsFile);
            } else {
                providerRemover.removeAllRecords(providers);
            }
        }

        if(!cmd.hasOption(ONLY_RECORDS)) {
            providerRemover.removeAllDatasets(providers);
        }
    }

    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(PROVIDER_ID, "Provider id", false);
        commandLineHelper.addOption(PROVIDERS_FILE, "File with providers ids (text/plain; line by line)", false);
        commandLineHelper.addOption(RECORDS_FILE, "File with records to remove ids. It can be used instead provider-id or providers-file", false);
        commandLineHelper.addOption(URL, "URL to processing server", true);
        commandLineHelper.addOption(USERNAME, "User name for http authentication", true);
        commandLineHelper.addOption(PASSWORD, "Password  for http authentication", true);
        commandLineHelper.addOption(TEST_MODE, "Test mode flag (yes/no). If set list of data to remove will be only displayed - not removed!", false);
        commandLineHelper.addOption(ONLY_DATASETS, "If set, dataset will be removed only", false);
        commandLineHelper.addOption(ONLY_RECORDS, "If set, records will be removed only", false);

        return commandLineHelper.getOptions();
    }
}
