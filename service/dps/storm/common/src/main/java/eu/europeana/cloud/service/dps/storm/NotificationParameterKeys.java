package eu.europeana.cloud.service.dps.storm;

/**
 * Keys for Map of parameters.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationParameterKeys {
    //------- for BASIC INFO -------
    public static final String TASK_STATE = "state";
    public static final String INFO = "info";
    public static final String START_TIME = "start_time";
    public static final String FINISH_TIME = "finish_time";
    public static final String TOPOLOGY_NAME = "topology_name";


    //------- for RELATION -------
    public static final String CHILD_TASK_ID = "childTaskId";

    //------- for NOTIFICATION -------
    public static final String RESOURCE = "resource";
    public static final String STATE = "state";
    public static final String INFO_TEXT = "info_text";
    public static final String ADDITIONAL_INFORMATIONS = "additionalInfo";
    public static final String RESULT_RESOURCE = "resultResource";

    public static final String EXCEPTION_ERROR_MESSAGE = "EXCEPTION_ERROR_MESSAGE";
    public static final String UNIFIED_ERROR_MESSAGE = "UNIFIED_ERROR_MESSAGE";

    public static final String DATA_SET_CLEANING_PARAMETERS = "DATA_SET_CLEANING_PARAMETERS";
    public static final String DPS_URL = "DPS_URL";

}
