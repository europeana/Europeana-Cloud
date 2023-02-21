package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.enrichment.rest.client.report.Report;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Tuple for notifications utilities.
 */
public class NotificationTuple {

  public static final String TASK_ID_FIELD_NAME = "TASK_ID";
  public static final String PARAMETERS_FIELD_NAME = "PARAMETERS";
  public static final String REPORT_SET_FIELD_NAME = "REPORT_SET";


  private final long taskId;
  private final Map<String, Object> parameters;
  private final Set<Report> reportSet = new HashSet<>();

  public NotificationTuple(long taskId, Map<String, Object> parameters) {
    this.taskId = taskId;
    this.parameters = parameters;
  }

  public NotificationTuple(long taskId, Map<String, Object> parameters, Set<Report> reports) {
    this(taskId, parameters);
    reportSet.addAll(reports);

  }


  @SuppressWarnings("unchecked")
  public static NotificationTuple fromStormTuple(Tuple tuple) {
    return new NotificationTuple(tuple.getLongByField(TASK_ID_FIELD_NAME),
        (Map<String, Object>) tuple.getValueByField(PARAMETERS_FIELD_NAME),
        (Set<Report>) tuple.getValueByField(REPORT_SET_FIELD_NAME));
  }

  public static NotificationTuple prepareNotification(StormTaskTuple stormTaskTuple, RecordState state, String message, String additionalInformation) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(NotificationParameterKeys.RESOURCE, stormTaskTuple.getFileUrl());
    parameters.put(NotificationParameterKeys.STATE, state.toString());
    parameters.put(NotificationParameterKeys.INFO_TEXT, message);
    parameters.put(NotificationParameterKeys.STATE_DESCRIPTION, additionalInformation);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
    if (stormTaskTuple.isMarkedAsDeleted()) {
      parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
    }
    return new NotificationTuple(stormTaskTuple.getTaskId(), parameters, stormTaskTuple.getReportSet());
  }


  public static NotificationTuple prepareNotificationWithResultResource(StormTaskTuple stormTaskTuple, RecordState state, String message, String additionalInformation) {
    NotificationTuple nt = prepareNotification(stormTaskTuple, state, message, additionalInformation);
    nt.addParameter(NotificationParameterKeys.RESULT_RESOURCE, stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_URL));
    return nt;
  }

  public static NotificationTuple prepareNotificationWithResultResourceAndErrorMessage(StormTaskTuple stormTaskTuple, RecordState state, String message, String additionalInformation, String unifiedErrorMessage, String detailedErrorMessage) {
    NotificationTuple nt = prepareNotificationWithResultResource(stormTaskTuple, state, message, additionalInformation);
    nt.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, unifiedErrorMessage);
    nt.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, detailedErrorMessage);
    return nt;
  }


  public static NotificationTuple prepareIndexingNotification(StormTaskTuple stormTaskTuple,
                                                              RecordState state, String message,
                                                              String additionalInformation) {
    NotificationTuple nt = prepareNotification(stormTaskTuple, state, message, additionalInformation);
    nt.addParameter(NotificationParameterKeys.EUROPEANA_ID, stormTaskTuple.getParameter(PluginParameterKeys.EUROPEANA_ID));
    return nt;
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

  public static Fields getFields() {
    return new Fields(TASK_ID_FIELD_NAME, PARAMETERS_FIELD_NAME, REPORT_SET_FIELD_NAME);
  }

  public Set<Report> getReportSet() {
    return reportSet;
  }

  public void addReports(Collection<Report> reports) {
    reportSet.addAll(reports);
  }

  public Values toStormTuple() {
    return new Values(taskId, parameters, reportSet);
  }

  public boolean isMarkedAsDeleted() {
    return "true".equals(parameters.get(PluginParameterKeys.MARKED_AS_DELETED));
  }

  public boolean isIgnoredRecord() {
    return "true".equals(parameters.get(PluginParameterKeys.IGNORED_RECORD));
  }

  public String getResource() {
    return String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
  }
}
