package eu.europeana.cloud.service.dps.storm.tuple.storm;


import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.enrichment.rest.client.report.Report;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.time.format.DateTimeParseException;
import java.util.*;

import static eu.europeana.cloud.service.dps.storm.StormTupleKeys.*;


/**
 * Storm Tuple that is aware of the DpsTask it is part of.
 * <p/>
 * Useful to track progress of a Task as tuples emitted from different tasks are being processed.
 *
 * @author manos
 */
@Setter
@Getter
public class StormTaskTuple implements Serializable {

  private static final long serialVersionUID = 1L;

  private Map<String, String> parameters;
  private OAIPMHHarvestingDetails sourceDetails;
  private String throttlingGroupingAttribute;
  @Delegate
  private ProcessingMetadata processingMetadata;
  @Delegate
  private FileMetadata fileMetadata;
  @Delegate
  private TaskMetadata taskMetadata;

  @Builder
  public StormTaskTuple(TaskMetadata taskMetadata,
                        FileMetadata fileMetadata,
                        ProcessingMetadata processingMetadata,
                        Map<String, String> parameters,
                        OAIPMHHarvestingDetails sourceDetails,
                        String throttlingGroupingAttribute) {
    this.taskMetadata = taskMetadata;
    this.fileMetadata = fileMetadata;
    this.processingMetadata = processingMetadata;
    this.parameters = parameters;
    this.sourceDetails = sourceDetails;
    this.throttlingGroupingAttribute = throttlingGroupingAttribute;
  }

  public StormTaskTuple(TaskMetadata taskMetadata,
                        Map<String, String> parameters,
                        OAIPMHHarvestingDetails harvestingDetails) {
    this.taskMetadata = taskMetadata;
    this.parameters = parameters;
    this.sourceDetails = harvestingDetails;
  }

  public StormTaskTuple() {
    this.parameters = new HashMap<>();
    this.taskMetadata = new TaskMetadata();
    this.fileMetadata = new FileMetadata();
    this.processingMetadata = new ProcessingMetadata();
  }

  public static StormTaskTuple fromStormTuple(Tuple tuple) {
    return StormTaskTuple.builder()
            .taskMetadata(
                    new TaskMetadata(tuple.getLongByField(TASK_ID_TUPLE_KEY),
                            tuple.getStringByField(RECORD_URI_TUPLE_KEY),
                            tuple.getStringByField(TASK_NAME_TUPLE_KEY),
                            tuple.getIntegerByField(RECORD_ATTEMPT_NUMBER)))
            .fileMetadata(
                    new FileMetadata(tuple.getStringByField(INPUT_FILES_TUPLE_KEY),
                            tuple.getBinaryByField(FILE_CONTENT_TUPLE_KEY)))

            .parameters((HashMap<String, String>) tuple.getValueByField(PARAMETERS_TUPLE_KEY))
            .processingMetadata(
                    new ProcessingMetadata(
                            (HashSet<Report>) tuple.getValueByField(REPORT_SET_TUPLE_KEY)))
                         .sourceDetails((OAIPMHHarvestingDetails) tuple.getValueByField(SOURCE_TO_HARVEST))
                         .build();
  }

  public void addParameter(String parameterKey, String parameterValue) {
    parameters.put(parameterKey, parameterValue);
  }

  public String getParameter(String parameterKey) {
    return parameters.get(parameterKey);
  }

  public Boolean ifParametersContainsKey(String parameterKey){
    return parameters.containsKey(parameterKey);
  }



  public static StormTaskTuple fromValues(List<Object> list) {
    return StormTaskTuple.builder()
            .taskMetadata(new TaskMetadata(
                    (Long) list.get(0), (String) list.get(10), (String) list.get(1), (Integer) list.get(7)
            ))
            .fileMetadata(new FileMetadata(
                    (String) list.get(2), (byte[]) list.get(3))
            )
            .processingMetadata(new ProcessingMetadata(
                    (HashSet<Report>) list.get(8)
            ))
            .parameters((Map<String, String>) list.get(4))
            .sourceDetails((OAIPMHHarvestingDetails) list.get(6))
            .build();
  }

  public static Fields getFields() {
    return new Fields(
            TASK_ID_TUPLE_KEY,
            TASK_NAME_TUPLE_KEY,
            INPUT_FILES_TUPLE_KEY,
            FILE_CONTENT_TUPLE_KEY,
            PARAMETERS_TUPLE_KEY,
            REVISIONS,
            SOURCE_TO_HARVEST,
            RECORD_ATTEMPT_NUMBER,
            THROTTLING_GROUPING_ATTRIBUTE,
            REPORT_SET_TUPLE_KEY,
            RECORD_URI_TUPLE_KEY
    );
  }


  public Values toStormTuple() {
    return new Values(
            taskMetadata.getTaskId(), taskMetadata.getTaskName(),
            fileMetadata.getFileUrl(), fileMetadata.getFileData(),
            parameters,
            processingMetadata.getOutputRevision(), sourceDetails,
            taskMetadata.getRecordAttemptNumber(),
            throttlingGroupingAttribute,
            processingMetadata.getReportSet(),
            taskMetadata.getRecordUri());
  }

  public boolean isMarkedAsDeleted() {
    return "true".equals(parameters.get(PluginParameterKeys.MARKED_AS_DELETED));
  }

  public void setMarkedAsDeleted(boolean markedAsDeleted) {
    if (markedAsDeleted) {
      parameters.put(PluginParameterKeys.MARKED_AS_DELETED, "true");
    } else {
      parameters.remove(PluginParameterKeys.MARKED_AS_DELETED);
    }
  }

  public int readParallelizationParam() {
    return Optional.ofNullable(getParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION))
                   .map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  public Date getHarvestDate() throws DateTimeParseException {
    return Date.from(DateHelper.parse(getParameter(PluginParameterKeys.HARVEST_DATE)));
  }
}
