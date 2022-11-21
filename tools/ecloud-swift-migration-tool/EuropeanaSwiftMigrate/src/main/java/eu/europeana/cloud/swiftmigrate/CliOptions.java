package eu.europeana.cloud.swiftmigrate;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Class builds cli {@link Options}.
 */
public class CliOptions {

  private final Options options = new Options();


  /**
   * Method adds required option.
   *
   * @param commandString Name of option
   * @throws IllegalArgumentException
   */
  public void addCliSetOption(final String commandString)
      throws IllegalArgumentException {
    options.addOption(OptionBuilder.withArgName(commandString).hasArgs(1).isRequired(true)
                                   .withDescription("set " + commandString).create(commandString));
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
