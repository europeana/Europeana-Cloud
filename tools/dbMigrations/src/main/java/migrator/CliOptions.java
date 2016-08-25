package migrator;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Class builds cli {@link Options}.
 *
 * @author krystian.
 */
public class CliOptions {

    private final Options options = new Options();


    /**
     * Method adds required option.
     *
     * @param commandString Name of option
     * @throws IllegalArgumentException
     */
    public void addCliSetRequiredOption(final String commandString, final String description)
            throws IllegalArgumentException {
        options.addOption(OptionBuilder.withArgName(commandString).hasArgs(1).isRequired(true)
                .withDescription("set " + description).create(commandString));
    }

    public void addCliSetRequiredOption(final String commandString, final String description, final boolean isRequired)
            throws IllegalArgumentException {
        options.addOption(OptionBuilder.withArgName(commandString).hasArgs(1).isRequired(isRequired)
                .withDescription("set " + description).create(commandString));
    }




    /**
     * Gets {@link Options}.
     *
     * @return {@link Options}
     */
    public Options getOptions() {
        return options;
    }
}

