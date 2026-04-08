package eu.europeana.cloud.common.utils;


import java.util.regex.Pattern;

/**
 * Wrapper class for operations related with cleaning messages that goes to the log file
 */
public final class LogMessageCleaner {
  private static final Pattern REPLACEABLE_CRLF_CHARACTERS_REGEX = Pattern.compile("[\r\n\t]");

  private LogMessageCleaner() {
  }

  /**
   * Cleans the log massage that will go to the log file
   *
   * @param value value to be cleaned
   * @return cleaned value
   */
  public static String clean(Object value) {
    return REPLACEABLE_CRLF_CHARACTERS_REGEX.matcher(value == null ? "null" : value.toString()).replaceAll("");
  }
}
