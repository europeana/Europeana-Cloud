package eu.europeana.cloud.service.dps.storm.tuple.storm;


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
  private String throttlingGroupingAttribute;
  @Delegate
  private StormProcessingMetadata stormProcessingMetadata;
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
                        StormProcessingMetadata stormProcessingMetadata,
                        Map<String, String> parameters,
                        String throttlingGroupingAttribute) {
    this.taskMetadata = taskMetadata;
    this.fileMetadata = fileMetadata;
    this.processingMetadata = processingMetadata;
    this.stormProcessingMetadata = stormProcessingMetadata;
    this.parameters = parameters;
    this.throttlingGroupingAttribute = throttlingGroupingAttribute;
  }

  public StormTaskTuple(TaskMetadata taskMetadata,
                        StormProcessingMetadata stormProcessingMetadata,
                        FileMetadata fileMetadata) {
    this.taskMetadata = taskMetadata;
    this.stormProcessingMetadata = stormProcessingMetadata;
    this.fileMetadata = fileMetadata;
    this.processingMetadata = new ProcessingMetadata();
    this.parameters = new HashMap<>();
  }

  public StormTaskTuple() {
    this.parameters = new HashMap<>();
    this.taskMetadata = new TaskMetadata();
    this.fileMetadata = new FileMetadata();
    this.processingMetadata = new ProcessingMetadata();
    this.stormProcessingMetadata = new StormProcessingMetadata();
  }

  public static StormTaskTuple fromStormTuple(Tuple tuple) {
    return StormTaskTuple.builder()
            .taskMetadata(
                    (TaskMetadata) tuple.getValueByField(TASK_METADATA_TUPLE_KEY))
            .stormProcessingMetadata(
                    (StormProcessingMetadata) tuple.getValueByField(STORM_PROCESSING_METADATA_TUPLE_KEY))
            .fileMetadata(
                    (FileMetadata) tuple.getValueByField(FILE_METADATA_TUPLE_KEY))
            .processingMetadata(
                    (ProcessingMetadata) tuple.getValueByField(PROCESSING_METADATA_TUPLE_KEY))
            .parameters((HashMap<String, String>) tuple.getValueByField(PARAMETERS_TUPLE_KEY))
            .build();
  }

  public void addParameter(String parameterKey, String parameterValue) {
    parameters.put(parameterKey, parameterValue);
  }

  public void addParameters(Map<String, String> parameters) {
    this.parameters.putAll(parameters);
  }

  public String getParameter(String parameterKey) {
    return parameters.get(parameterKey);
  }

  public Boolean ifParametersContainsKey(String parameterKey) {
    return parameters.containsKey(parameterKey);
  }



  public static StormTaskTuple fromValues(List<Object> list) {
    return StormTaskTuple.builder()
            .taskMetadata((TaskMetadata) list.get(1))
            .processingMetadata((ProcessingMetadata) list.get(2))
            .stormProcessingMetadata((StormProcessingMetadata) list.get(3))
            .fileMetadata((FileMetadata) list.get(4))
            .parameters((Map<String, String>) list.get(5))
            .build();
  }

  public static Fields getFields() {
    return new Fields(
            TASK_ID_TUPLE_KEY,
            TASK_METADATA_TUPLE_KEY,
            PROCESSING_METADATA_TUPLE_KEY,
            STORM_PROCESSING_METADATA_TUPLE_KEY,
            FILE_METADATA_TUPLE_KEY,
            PARAMETERS_TUPLE_KEY
    );
  }


  public Values toStormTuple() {
    return new Values(
            taskMetadata.getTaskId(),
            taskMetadata,
            processingMetadata,
            stormProcessingMetadata,
            fileMetadata,
            parameters
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
