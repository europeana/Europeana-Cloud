package eu.europeana.cloud.util;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 * Created by Tarek on 9/1/2016.
 */
public class CommandLineHelper {
    private Options options;

    public CommandLineHelper() {

        options = new Options();
    }

    /**
     * adds required option.
     *
     * @param optionName  Name of the  required option
     * @param description description of the option
     * @throws IllegalArgumentException
     */
    public void addRequiredOption(final String optionName, final String description)
            throws IllegalArgumentException {
        Option option = Option.builder().longOpt(optionName).required().desc(description).hasArg().build();
        options.addOption(option);
    }

    public Options getOptions() {
        return options;
    }
}




