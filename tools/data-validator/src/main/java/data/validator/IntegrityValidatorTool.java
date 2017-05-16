package data.validator;

import data.validator.constants.ValidatorType;
import data.validator.properities.PropertyFileLoader;
import data.validator.utils.CommandLineHelper;
import data.validator.validator.Validator;
import data.validator.validator.ValidatorFactory;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.Properties;

import static data.validator.constants.Constants.*;

/**
 * Created by Tarek on 4/27/2017.
 */
public class IntegrityValidatorTool {
    private static Properties topologyProperties;
    final static Logger LOGGER = Logger.getLogger(IntegrityValidatorTool.class);

    public static void main(String[] args) {
        topologyProperties = new Properties();
        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            executeIntegrityValidation(cmd);
        } catch (ParseException exp) {
            System.out.println(exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Integrity Validator ", options);

        } catch (Exception e) {
            LOGGER.error("An exception happened caused by: " + e.getMessage());
            System.exit(1);
        }
    }

    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(SOURCE_TABLE, "source table", false);
        commandLineHelper.addOption(TARGET_TABLE, "target table", false);
        commandLineHelper.addOption(THREADS_COUNT, "threads count (int)(optional)(default=10)", false);
        commandLineHelper.addOption(CONFIGURATION_PROPERTIES, "properties file to configure source and target databases", false);
        return commandLineHelper.getOptions();
    }

    private static void executeIntegrityValidation(CommandLine cmd) throws Exception {
        String configurationFileName = cmd.getOptionValue(CONFIGURATION_PROPERTIES);
        PropertyFileLoader.loadPropertyFile(DEFAULT_PROPERTIES_FILE, configurationFileName, topologyProperties);

        CassandraConnectionProvider sourceCassandraConnectionProvider = getCassandraConnectionProviderFromConfiguration(SOURCE_HOSTS, SOURCE_PORT, SOURCE_KEYSPACE, SOURCE_USER_NAME, SOURCE_PASSWORD);
        CassandraConnectionProvider targetCassandraConnectionProvider = getCassandraConnectionProviderFromConfiguration(TARGET_HOSTS, TARGET_PORT, TARGET_KEYSPACE, TARGET_USER_NAME, TARGET_PASSWORD);
        String sourceTable = cmd.getOptionValue(SOURCE_TABLE);
        String targetTable = cmd.getOptionValue(TARGET_TABLE);

        ValidatorType validatorType = ValidatorType.TABLE;
        if ((sourceTable == null) && (targetTable != null))
            sourceTable = targetTable;
        else if ((sourceTable != null) && (targetTable == null))
            targetTable = sourceTable;
        else if ((sourceTable == null) && (targetTable == null))
            validatorType = ValidatorType.KEYSPACE;

        String threads = cmd.getOptionValue(THREADS_COUNT);
        int threadsCount = DEFAULT_THREADS_COUNT;
        if (threads != null)
            threadsCount = Integer.parseInt(threads);

        Validator validator = ValidatorFactory.getValidator(validatorType);
        validator.validate(sourceCassandraConnectionProvider, targetCassandraConnectionProvider, sourceTable, targetTable, threadsCount);
    }

    private static CassandraConnectionProvider getCassandraConnectionProviderFromConfiguration(String hosts, String port, String keyspace, String userName, String password) {
        return new CassandraConnectionProvider(topologyProperties.getProperty(hosts),
                Integer.parseInt(topologyProperties.getProperty(port)), topologyProperties.getProperty(keyspace), topologyProperties.getProperty(userName)
                , topologyProperties.getProperty(password));
    }
}
