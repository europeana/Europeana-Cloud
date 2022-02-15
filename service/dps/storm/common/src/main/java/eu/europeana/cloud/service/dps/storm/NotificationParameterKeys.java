package eu.europeana.cloud.service.dps.storm;

/**
 * Keys for Map of parameters.
 */
public class NotificationParameterKeys {
    //------- for BASIC INFO -------
    public static final String TOPOLOGY_NAME = "topology_name";


    //------- for NOTIFICATION -------
    public static final String RESOURCE = "resource";
    public static final String STATE = "state";
    public static final String INFO_TEXT = "info_text";
    public static final String ADDITIONAL_INFORMATION = "ADDITIONAL_INFORMATION";
    public static final String RECORD_ID = "RECORD_ID";
    public static final String RESULT_RESOURCE = "resultResource";

    public static final String EXCEPTION_ERROR_MESSAGE = "EXCEPTION_ERROR_MESSAGE";
    public static final String UNIFIED_ERROR_MESSAGE = "UNIFIED_ERROR_MESSAGE";

    public static final String DATA_SET_CLEANING_PARAMETERS = "DATA_SET_CLEANING_PARAMETERS";
    public static final String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";

    private NotificationParameterKeys() {
    }
}
