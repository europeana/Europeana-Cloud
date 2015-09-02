package eu.europeana.cloud.service.dps.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 * Tuple for notifications.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationTuple 
{
    public static final String taskIdFieldName = "TASK_ID";
    public static final String informationTypeFieldName = "INFORMATION_TYPE";
    public static final String parametersFieldName = "PARAMETERS";
    
    public enum InformationTypes
    {
        BASIC_INFO,
        NOTIFICATION
    }
    
    public enum States
    {
        SUCCESS,
        DROPPED,
        KILLED,
        ERROR,
        
        FINISHED    //this status is used by notification bolt when whole DPS taks is processed
    }
    
    private final long taskId;
    private final InformationTypes informationType;
    private final Map<String, String> parameters;

    protected NotificationTuple(long taskId, InformationTypes informationType, Map<String, String> parameters) 
    {
        this.taskId = taskId;
        this.informationType = informationType;
        this.parameters = parameters;
    }
    
    public static NotificationTuple prepareBasicInfo(long taskId, int expectedSize)
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.EXPECTED_SIZE, String.valueOf(expectedSize));
        
        return new NotificationTuple(taskId, InformationTypes.BASIC_INFO, parameters);
    }
    
    public static NotificationTuple prepareNotification(long taskId, String resource, 
            States state, String text, String additionalInformations)
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        
        return new NotificationTuple(taskId, InformationTypes.NOTIFICATION, parameters);
    }

    public long getTaskId() 
    {
        return taskId;
    }

    public InformationTypes getInformationType() 
    {
        return informationType;
    }

    public Map<String, String> getParameters() 
    {
        return parameters;
    }
    
    public String getParameter(String key)
    {
        return parameters.get(key);
    }
    
    public void addParameter(String key, String value)
    {
        parameters.put(key, value);
    }
    
    public static NotificationTuple fromStormTuple(Tuple tuple)
    {
        return new NotificationTuple(tuple.getLongByField(taskIdFieldName),
                        (InformationTypes)tuple.getValueByField(informationTypeFieldName),
                        (Map<String, String>)tuple.getValueByField(parametersFieldName));
    }
    
    public Values toStormTuple()
    {
        return new Values(taskId, informationType, parameters);
    }
    
    public static Fields getFields()
    {
        return new Fields(taskIdFieldName, informationTypeFieldName, parametersFieldName);
    }
}
