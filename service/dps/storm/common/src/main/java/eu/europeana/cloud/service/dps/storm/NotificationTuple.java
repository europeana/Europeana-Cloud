package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
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

  public static NotificationTuple prepareNotification(long taskId, boolean markedAsDeleted, String resource,
      RecordState state, String text, String additionalInformation,
      long processingStartTime) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(NotificationParameterKeys.RESOURCE, resource);
    parameters.put(NotificationParameterKeys.STATE, state.toString());
    parameters.put(NotificationParameterKeys.INFO_TEXT, text);
    parameters.put(NotificationParameterKeys.STATE_DESCRIPTION, additionalInformation);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
    if (markedAsDeleted) {
      parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
    }

    return new NotificationTuple(taskId, parameters);
  }

  @SuppressWarnings("unchecked")
  public static NotificationTuple fromStormTuple(Tuple tuple) {
    return new NotificationTuple(tuple.getLongByField(TASK_ID_FIELD_NAME),
        (Map<String, Object>) tuple.getValueByField(PARAMETERS_FIELD_NAME),
        (Set<Report>) tuple.getValueByField(REPORT_SET_FIELD_NAME));
  }

  public static NotificationTuple prepareNotification(long taskId, boolean markedAsDeleted, String resource,
      RecordState state, String text, String additionalInformation, String resultResource,
      long processingStartTime) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(NotificationParameterKeys.RESOURCE, resource);
    parameters.put(NotificationParameterKeys.STATE, state.toString());
    parameters.put(NotificationParameterKeys.INFO_TEXT, text);
    parameters.put(NotificationParameterKeys.STATE_DESCRIPTION, additionalInformation);
    parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
    if (markedAsDeleted) {
      parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
    }

    return new NotificationTuple(taskId, parameters);
  }


  public static NotificationTuple prepareIndexingNotification(long taskId, boolean markedAsDeleted,
      String resource,
      RecordState state, String text,
      String additionalInformation, String europeanaId,
      String resultResource, long processingStartTime) {
    Map<String, Object> parameters = new HashMap<>();
    if (markedAsDeleted) {
      parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
    }
    parameters.put(NotificationParameterKeys.RESOURCE, resource);
    parameters.put(NotificationParameterKeys.STATE, state.toString());
    parameters.put(NotificationParameterKeys.INFO_TEXT, text);
    parameters.put(NotificationParameterKeys.STATE_DESCRIPTION, additionalInformation);
    parameters.put(NotificationParameterKeys.EUROPEANA_ID, europeanaId);
    parameters.put(NotificationParameterKeys.RESULT_RESOURCE, resultResource);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingStartTime);
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
