package eu.europeana.cloud.common.utils;

import eu.europeana.metis.utils.CommonStringValues;

/**
 * Wrapper class for operations related with cleaning messages that goes to the log file
 */
public final class LogMessageCleaner {

  private LogMessageCleaner() {
  }

  /**
   * Cleans the log massage that will go to the log file
   *
   * @param value value to be cleaned
   * @return cleaned value
   */
  public static String clean(Object value) {
    return CommonStringValues.CRLF_PATTERN.matcher(value == null ? "null" : value.toString()).replaceAll("");
  }
}
