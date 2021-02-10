package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.dps.InformationTypes;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Tuple for notifications.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationTuple {
    public static final String taskIdFieldName = "TASK_ID";
    public static final String informationTypeFieldName = "INFORMATION_TYPE";
    public static final String parametersFieldName = "PARAMETERS";


    private final long taskId;
    private final InformationTypes informationType;
    private final Map<String, Object> parameters;

    protected NotificationTuple(long taskId, InformationTypes informationType, Map<String, Object> parameters) {
        this.taskId = taskId;
        this.informationType = informationType;
        this.parameters = parameters;
    }


    public static NotificationTuple prepareUpdateTask(long taskId, String info, TaskState state, Date startTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.TASK_STATE, state.toString());
        parameters.put(NotificationParameterKeys.START_TIME, startTime);
        parameters.put(NotificationParameterKeys.INFO, info);
        return new NotificationTuple(taskId, InformationTypes.UPDATE_TASK, parameters);
    }

    public static NotificationTuple prepareEndTask(long taskId, String info, TaskState state, Date finishTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.TASK_STATE, state.toString());
        parameters.put(NotificationParameterKeys.FINISH_TIME, finishTime);
        parameters.put(NotificationParameterKeys.INFO, info);
        return new NotificationTuple(taskId, InformationTypes.END_TASK, parameters);
    }


    public static NotificationTuple prepareNotification(long taskId, String resource,
                                                        RecordState state, String text, String additionalInformations,
                                                        long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);

        return new NotificationTuple(taskId, InformationTypes.NOTIFICATION, parameters);
    }

    public static NotificationTuple prepareNotification(long taskId, String resource,
                                                        RecordState state, String text, String additionalInformations, String resultResource,
                                                        long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);

        return new NotificationTuple(taskId, InformationTypes.NOTIFICATION, parameters);
    }


    public static NotificationTuple prepareIndexingNotification(long taskId, DataSetCleanerParameters dataSetCleanerParameters, String dpsURL,
                                                                String authenticationHeader, String resource, RecordState state, String text,
                                                                String additionalInformations, String resultResource,
                                                                long processingStartTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
        parameters.put(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS, dataSetCleanerParameters);
        parameters.put(NotificationParameterKeys.DPS_URL, dpsURL);
        parameters.put(NotificationParameterKeys.AUTHORIZATION_HEADER, authenticationHeader);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
        return new NotificationTuple(taskId, InformationTypes.NOTIFICATION, parameters);
    }

    public long getTaskId() {
        return taskId;
    }

    public InformationTypes getInformationType() {
        return informationType;
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
        return new NotificationTuple(tuple.getLongByField(taskIdFieldName),
                (InformationTypes) tuple.getValueByField(informationTypeFieldName),
                (Map<String, Object>) tuple.getValueByField(parametersFieldName));
    }

    public Values toStormTuple() {
        return new Values(taskId, informationType, parameters);
    }

    public static Fields getFields() {
        return new Fields(taskIdFieldName, informationTypeFieldName, parametersFieldName);
    }

    public boolean isRecordDeleted() {
        return "true".equals(parameters.get(PluginParameterKeys.DELETED_RECORD));
    }

    public void setRecordDeleted(boolean recordDeleted) {
        if(recordDeleted){
            parameters.put(PluginParameterKeys.DELETED_RECORD,"true");
        }else{
            parameters.remove(PluginParameterKeys.DELETED_RECORD);
        }
    }
}
