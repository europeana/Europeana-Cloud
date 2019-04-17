package eu.europeana.cloud;

import eu.europeana.cloud.api.Remover;
import eu.europeana.cloud.executer.RemoverInvoker;
import eu.europeana.cloud.persisted.RemoverImpl;
import eu.europeana.cloud.utils.CommandLineHelper;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Tarek on 4/16/2019.
 */
public class TaskCleanerTool {

    static final Logger LOGGER = Logger.getLogger(TaskCleanerTool.class);

    private static final String HOSTS = "hosts";
    private static final String PORT = "port";
    private static final String KEYSPACE = "keyspace";
    private static final String USER_NAME = "username";
    private static final String PASSWORD = "password";
    private static final String TASK_ID = "taskId";
    private static final String TASK_IDS_FILE_PATH = "task_Ids_file_path";
    private static final String REMOVE_ERROR_REPORTS = "remove_error_reports";


    public static void main(String[] args) {
        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            LOGGER.info("Starting the cleaning");
            CommandLine cmd = parser.parse(options, args);
            Remover remover = buildRemover(cmd);
            executeRemoval(cmd, remover);
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

    private static void executeRemoval(CommandLine cmd, Remover remover) throws IOException {
        RemoverInvoker removerInvoker = new RemoverInvoker(remover);
        if (cmd.getOptionValue(TASK_ID) == null && cmd.getOptionValue(TASK_IDS_FILE_PATH) == null)
            throw new IllegalArgumentException("you should pass either " + TASK_ID + " or " + TASK_IDS_FILE_PATH + " to define the task/tasks to remove");
        if (cmd.getOptionValue(TASK_IDS_FILE_PATH) != null)
            removerInvoker.executeInvokerForListOfTasks(cmd.getOptionValue(TASK_IDS_FILE_PATH), Boolean.parseBoolean(cmd.getOptionValue(REMOVE_ERROR_REPORTS)));
        else
            removerInvoker.executeInvokerForSingleTask(Long.parseLong(cmd.getOptionValue(TASK_ID)), Boolean.parseBoolean(cmd.getOptionValue(REMOVE_ERROR_REPORTS)));
    }

    private static Remover buildRemover(CommandLine cmd) {
        return new RemoverImpl(cmd.getOptionValue(HOSTS), Integer.parseInt(cmd.getOptionValue(PORT)),
                cmd.getOptionValue(KEYSPACE), cmd.getOptionValue(USER_NAME), cmd.getOptionValue(PASSWORD));
    }


    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(HOSTS, "hosts", true);
        commandLineHelper.addOption(PORT, "port (int)", true);
        commandLineHelper.addOption(KEYSPACE, "DPS keyspace", true);
        commandLineHelper.addOption(USER_NAME, "user name", true);
        commandLineHelper.addOption(PASSWORD, "password", true);
        commandLineHelper.addOption(REMOVE_ERROR_REPORTS, "remove error reports? (boolean) (true/false)", true);
        commandLineHelper.addOption(TASK_ID, "Task Id (long)", false);
        commandLineHelper.addOption(TASK_IDS_FILE_PATH, "path to task ids csv or txt file (one taskId per line)(String) ", false);

        return commandLineHelper.getOptions();
    }
}
