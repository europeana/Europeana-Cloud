package eu.europeana.cloud.service.dps.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.common.model.dps.InformationTypes;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;

import java.text.SimpleDateFormat;
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

    public static NotificationTuple prepareBasicInfo(long taskId, int expectedSize, TaskState state, String info, Date startTime, Date finishTime) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.EXPECTED_SIZE, String.valueOf(expectedSize));
        parameters.put(NotificationParameterKeys.TASK_STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO, info);
        parameters.put(NotificationParameterKeys.START_TIME, startTime);
        parameters.put(NotificationParameterKeys.FINISH_TIME, finishTime);
        return new NotificationTuple(taskId, InformationTypes.BASIC_INFO, parameters);
    }

    public static NotificationTuple prepareNotification(long taskId, String resource,
                                                        States state, String text, String additionalInformations) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);

        return new NotificationTuple(taskId, InformationTypes.NOTIFICATION, parameters);
    }

    public static NotificationTuple prepareNotification(long taskId, String resource,
                                                        States state, String text, String additionalInformations, String resultResource) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(NotificationParameterKeys.RESOURCE, resource);
        parameters.put(NotificationParameterKeys.STATE, state.toString());
        parameters.put(NotificationParameterKeys.INFO_TEXT, text);
        parameters.put(NotificationParameterKeys.ADDITIONAL_INFORMATIONS, additionalInformations);
        parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
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
}
