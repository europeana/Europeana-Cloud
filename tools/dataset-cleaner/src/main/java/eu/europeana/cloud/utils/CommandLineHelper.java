package eu.europeana.cloud.utils;

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
     */
    public final void addOption(final String optionName, final String description,boolean isRequired) {
        Option option = Option.builder().longOpt(optionName).required(isRequired).desc(description).hasArg().build();
        options.addOption(option);
    }

    public final Options getOptions() {
        return options;
    }
}




