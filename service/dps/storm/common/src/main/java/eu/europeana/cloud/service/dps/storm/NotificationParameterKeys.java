package eu.europeana.cloud.service.dps.storm;

/**
 * Keys for Map of parameters.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationParameterKeys {
    //------- for BASIC INFO -------
    public static final String EXPECTED_SIZE = "expectedSize";
    public static final String TASK_STATE = "state";
    public static final String INFO = "info";
    public static final String START_TIME ="start_time";
    public static final String FINISH_TIME ="finish_time";


    //------- for RELATION -------
    public static final String CHILD_TASK_ID = "childTaskId";

    //------- for NOTIFICATION -------
    public static final String RESOURCE = "resource";
    public static final String STATE = "state";
    public static final String INFO_TEXT = "info_text";
    public static final String ADDITIONAL_INFORMATIONS = "additionalInfo";
    public static final String RESULT_RESOURCE = "resultResource";
}
