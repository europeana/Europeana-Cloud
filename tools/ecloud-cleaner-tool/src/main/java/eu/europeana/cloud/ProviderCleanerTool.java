package eu.europeana.cloud;

import eu.europeana.cloud.api.Remover;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.persisted.ProviderRemover;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.utils.CommandLineHelper;
import eu.europeana.cloud.utils.Toolkit;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.ArrayList;
import java.util.List;

public class ProviderCleanerTool {
    static final Logger LOGGER = Logger.getLogger(TaskCleanerTool.class);

    private static final String URL = "url";
    private static final String TEST_MODE = "test-mode";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String PROVIDER_ID = "provider-id";
    private static final String PROVIDERS_FILE = "providers-file";

// --provider-id metis_test5  --url https://test.ecloud.psnc.pl/api --username metis_test --password ******** --test-mode yes
// --provider-id ola-test --url http://ecloud.psnc.pl/api --username metis_production --password ******** --test-mode yes

//--provider-id ola-test  --url http://ecloud.psnc.pl/api --username metis_production --password ******** --test-mode yes
//--providers-file /home/user/path/to/file/with/identifiers/providers.subset.txt --url http://ecloud.psnc.pl/api --username metis_production --password ******** --test-mode yes

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
            LOGGER.error("Error while cleaning data " + e.getMessage() + ". Because of " + e.getCause());
            System.exit(1);
        }
    }

    private static void remove(CommandLine cmd) throws CloudException, MCSException {
        String providersFilename = cmd.getOptionValue(PROVIDERS_FILE, null);
        List<String> providers = null;
        if(providersFilename != null) {
            providers = Toolkit.readIdentifiers(providersFilename);
        } else {
            String singleProvider = cmd.getOptionValue(PROVIDER_ID, null);
            if(singleProvider != null) {
                providers = new ArrayList<>();
                providers.add(singleProvider);
            } else {
                LOGGER.error("At least one provider id must be provided (nomen omen) to this tool program");
                System.exit(-1);
            }
        }

        ProviderRemover providerRemover = new ProviderRemover(
                cmd.getOptionValue(URL),
                cmd.getOptionValue(USERNAME),
                cmd.getOptionValue(PASSWORD),
                cmd.hasOption(TEST_MODE)
        );

        providerRemover.removeAllRecords(providers);
        providerRemover.removeAllDatasets(providers);
    }


    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(PROVIDER_ID, "Provider id", false);
        commandLineHelper.addOption(PROVIDERS_FILE, "File with providers ids (text/plain; line by line)", false);
        commandLineHelper.addOption(URL, "URL to processing server", true);
        commandLineHelper.addOption(USERNAME, "User name for http authentication", true);
        commandLineHelper.addOption(PASSWORD, "Password  for http authentication", true);
        commandLineHelper.addOption(TEST_MODE, "Test mode flag (yes/no). If set list of data to remove will be only displayed - not removed!", false);

        return commandLineHelper.getOptions();
    }
}
