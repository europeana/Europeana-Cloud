package eu.europeana.cloud.service.dps.storm.tuple.common;


import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
public class CommonTaskTuple implements Serializable {

  private static final long serialVersionUID = 1L;

  @Delegate
  private processingData processingData;
  @Delegate
  private RecordData fileMetadata;
  @Delegate
  private TaskData taskData;

  @Builder
  public CommonTaskTuple(TaskData taskData,
                         RecordData recordData,
                         processingData processingData) {
    this.taskData = taskData;
    this.processingData = processingData;
    this.fileMetadata = recordData;
  }

  public CommonTaskTuple() {
    this.taskData = new TaskData();
    this.fileMetadata = new RecordData();
    this.processingData = new processingData();
  }

  public static CommonTaskTuple fromStormTuple(Tuple tuple) {
    return CommonTaskTuple.builder()
            .taskData(
                    (TaskData) tuple.getValueByField(TASK_DATA_TUPLE_FIELD))
            .processingData(
                    (processingData) tuple.getValueByField(STORM_PROCESSING_DATA_TUPLE_FIELD))
            .recordData(
                    (RecordData) tuple.getValueByField(RECORD_DATA_TUPLE_FIELD))
            .build();
  }

  public void addParameter(String parameterKey, String parameterValue) {
    getParameters().put(parameterKey, parameterValue);
  }

  public void addParameters(Map<String, String> parameters) {
    getParameters().putAll(parameters);
  }

  public String getParameter(String parameterKey) {
    return getParameters().get(parameterKey);
  }

  public Boolean ifParametersContainsKey(String parameterKey) {
    return getParameters().containsKey(parameterKey);
  }



  public static CommonTaskTuple fromValues(List<Object> list) {
    return CommonTaskTuple.builder()
            .taskData((TaskData) list.get(3))
            .processingData((processingData) list.get(4))
            .recordData((RecordData) list.get(5))
            .build();
  }

  public static Fields getFields() {
    return new Fields(
            TASK_ID_TUPLE_FIELD,
            INPUT_FILES_TUPLE_FIELD,
            THROTTLING_GROUPING_ATTRIBUTE_TUPLE_FIELD,
            TASK_DATA_TUPLE_FIELD,
            STORM_PROCESSING_DATA_TUPLE_FIELD,
            RECORD_DATA_TUPLE_FIELD
    );
  }


  public Values toStormTuple() {
    return new Values(
            taskData.getTaskId(),
            fileMetadata.getRecordUri(),
            processingData.getThrottlingGroupingAttribute(),
            taskData,
            processingData,
            fileMetadata

    );
  }


  public int readParallelizationParam() {
    return Optional.ofNullable(getParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION))
                   .map(Integer::parseInt).orElse(Integer.MAX_VALUE);
  }

  public Date getHarvestDate() throws DateTimeParseException {
    return Date.from(DateHelper.parse(getParameter(PluginParameterKeys.HARVEST_DATE)));
  }
}
