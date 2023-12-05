package eu.europeana.cloud.service.dps.storm;

/**
 * Keys for Map of parameters.
 */
public final class NotificationParameterKeys {

  //------- for BASIC INFO -------
  public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";


  //------- for NOTIFICATION -------
  public static final String RESOURCE = "RESOURCE";
  public static final String STATE = "STATE";
  public static final String INFO_TEXT = "INFO_TEXT";
  public static final String STATE_DESCRIPTION = "STATE_DESCRIPTION";
  public static final String EUROPEANA_ID = "EUROPEANA_ID";
  public static final String RESULT_RESOURCE = "RESULT_RESOURCE";

  public static final String EXCEPTION_ERROR_MESSAGE = "EXCEPTION_ERROR_MESSAGE";
  public static final String UNIFIED_ERROR_MESSAGE = "UNIFIED_ERROR_MESSAGE";

  private NotificationParameterKeys() {
  }
}
