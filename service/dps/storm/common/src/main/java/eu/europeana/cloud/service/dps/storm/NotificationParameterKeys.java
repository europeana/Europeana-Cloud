package eu.europeana.cloud.service.dps.storm;

/**
 * Keys for Map of parameters.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationParameterKeys 
{
    //------- for BASIC INFO -------
    public static final String EXPECTED_SIZE = "expectedSize";
    
    //------- for RELATION -------
    public static final String CHILD_TASK_ID = "childTaskId";
    
    //------- for NOTIFICATION -------
    public static final String RESOURCE = "resource";
    public static final String STATE = "state";
    public static final String INFO_TEXT = "info_text";
    public static final String ADDITIONAL_INFORMATIONS = "additionalInfo";
}
