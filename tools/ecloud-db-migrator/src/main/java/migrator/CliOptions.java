package migrator;

import org.apache.commons.cli.Option;
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
    options.addOption(Option.builder(commandString).argName(commandString).hasArg().required().desc("set " + description)
                            .build());
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

