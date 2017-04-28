package data.validator;

import data.validator.utils.CommandLineHelper;
import org.apache.commons.cli.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Tarek on 4/27/2017.
 */
public class IntegrityValidatorTool {
    private static final String SOURCE_TABLE = "sourceTable";
    private static final String TARGET_TABLE = "targetTable";
    private static final String THREADS_COUNT = "threads";
    private static final int DEFAULT_THREADS_COUNT = 10;

    public static void main(String[] args) {
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
            System.out.println("An exception happened caused by: " + e.getMessage());
        }

    }

    private static Options getOptions() {
        CommandLineHelper commandLineHelper = new CommandLineHelper();
        commandLineHelper.addOption(SOURCE_TABLE, "source table", false);
        commandLineHelper.addOption(TARGET_TABLE, "target table", false);
        commandLineHelper.addOption(THREADS_COUNT, "threads count (int)(optional)(default=10)", false);
        return commandLineHelper.getOptions();
    }

    private static void executeIntegrityValidation(CommandLine cmd) throws Exception {
        String sourceTable = cmd.getOptionValue(SOURCE_TABLE);
        String targetTable = cmd.getOptionValue(TARGET_TABLE);
        String threads = cmd.getOptionValue(THREADS_COUNT);
        int threadsCount = DEFAULT_THREADS_COUNT;
        if (threads != null)
            threadsCount = Integer.parseInt(threads);
        ApplicationContext context =
                new ClassPathXmlApplicationContext(new String[]{"data-validator-context.xml"});
        DataValidator dataValidator = (DataValidator) context.getBean("dataValidator");
        dataValidator.validate(sourceTable, targetTable, threadsCount);
    }
}
