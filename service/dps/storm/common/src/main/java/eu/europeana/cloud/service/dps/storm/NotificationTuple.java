package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.dps.InformationTypes;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Tuple for notifications.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationTuple {
    public static final String TASK_ID_FIELD_NAME = "TASK_ID";
    public static final String PARAMETERS_FIELD_NAME = "PARAMETERS";


    private final long taskId;
    private final Map<String, Object> parameters;

    protected NotificationTuple(long taskId, Map<String, Object> parameters) {
        this.taskId = taskId;
        this.parameters = parameters;
    }

    public static NotificationTuple prepareNotification(long taskId, boolean markedAsDeleted, String resource,
                                                        RecordState state, String text, String additionalInformations,
                                                        long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
        if (markedAsDeleted) {
            parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
        }

        return new NotificationTuple(taskId, parameters);
    }

    public static NotificationTuple prepareNotification(long taskId, boolean markedAsDeleted, String resource,
                                                        RecordState state, String text, String additionalInformations, String resultResource,
                                                        long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
        if (markedAsDeleted) {
            parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
        }

        return new NotificationTuple(taskId, parameters);
    }


    public static NotificationTuple prepareIndexingNotification(long taskId, boolean markedAsDeleted,
                                                                DataSetCleanerParameters dataSetCleanerParameters,
                                                                String authenticationHeader, String resource,
                                                                RecordState state, String text,
                                                                String additionalInformations, String resultResource,
                                                                long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
        parameters.put(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS, dataSetCleanerParameters);
        parameters.put(NotificationParameterKeys.AUTHORIZATION_HEADER, authenticationHeader);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
        if (markedAsDeleted) {
            parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
        }
        return new NotificationTuple(taskId, parameters);
    }

    public long getTaskId() {
        return taskId;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Object getParameter(String key) {
        return parameters.get(key);
    }

    public void addParameter(String key, String value) {
        parameters.put(key, value);
    }

    public static NotificationTuple fromStormTuple(Tuple tuple) {
        return new NotificationTuple(tuple.getLongByField(TASK_ID_FIELD_NAME),
                (Map<String, Object>) tuple.getValueByField(PARAMETERS_FIELD_NAME));
    }

    public Values toStormTuple() {
        return new Values(taskId, parameters);
    }

    public static Fields getFields() {
        return new Fields(TASK_ID_FIELD_NAME, PARAMETERS_FIELD_NAME);
    }

    public boolean isMarkedAsDeleted() {
        return "true".equals(parameters.get(PluginParameterKeys.MARKED_AS_DELETED));
    }

    public void setMarkedAsDeleted(boolean markedAsDeleted) {
        if(markedAsDeleted){
            parameters.put(PluginParameterKeys.MARKED_AS_DELETED,"true");
        }else{
            parameters.remove(PluginParameterKeys.MARKED_AS_DELETED);
        }
    }
}
