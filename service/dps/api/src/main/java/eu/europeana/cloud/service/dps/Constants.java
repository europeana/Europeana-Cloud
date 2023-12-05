package eu.europeana.cloud.service.dps;

public final class Constants {

  /**
   * Maximum number of errors (for one error type) that will be served to the client and stored in the database
   * (error_notifications)
   */
  public static final int MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE = 100;

  private Constants() {
  }

}
