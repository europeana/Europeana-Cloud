package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import java.io.IOException;
import java.io.Serializable;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.ToString;

@XmlRootElement
@ToString
public class DpsTask implements Serializable {

  private static final long serialVersionUID = 1L;

  /* Map of input data:
  cloud-records - InputDataType.FILE_URLS
  cloud-datasets - InputDataType.DATASET_URLS
  cloud-repositoryurl - InputDataType.REPOSITORY_URLS
  */
  private Map<InputDataType, List<String>> inputData;

  /* List of parameters (specific for each dps-topology) */
  private Map<String, String> parameters;

  /* output revision*/
  private Revision outputRevision;

  /* Unique id for this task */
  private long taskId;

  /* Name for the task */
  private String taskName;

  /**
   * Details of harvesting process
   */
  private OAIPMHHarvestingDetails harvestingDetails;


  public DpsTask() {
    this("");
  }

  /**
   * @param taskName
   */
  public DpsTask(String taskName) {

    this.taskName = taskName;

    inputData = new EnumMap<>(InputDataType.class);
    parameters = new HashMap<>();

    taskId = UUID.randomUUID().getMostSignificantBits();

    harvestingDetails = null;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  public Revision getOutputRevision() {
    return outputRevision;
  }

  public void setOutputRevision(Revision outputRevision) {
    this.outputRevision = outputRevision;
  }


  /**
   * @return Unique id for this task
   */
  public long getTaskId() {
    return taskId;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  /**
   * @return Name for the task
   */
  public String getTaskName() {
    return taskName;
  }

  public void addDataEntry(InputDataType dataType, List<String> data) {
    inputData.put(dataType, data);
  }

  public List<String> getDataEntry(InputDataType dataType) {
    return inputData.get(dataType);
  }

  public void addParameter(String parameterKey, String parameterValue) {
    parameters.put(parameterKey, parameterValue);
  }

  public String getParameter(String parameterKey) {
    return parameters.get(parameterKey);
  }

  /*
   * @return true if parameter is present and is not empty
   */
  public boolean isParameterPresent(String parameterKey){
    return parameters.containsKey(parameterKey) && !parameters.get(parameterKey).isBlank();
  }

  /**
   * @return List of parameters (specific for each dps-topology)
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return List of input data (cloud-records or cloud-datasets)
   */
  public Map<InputDataType, List<String>> getInputData() {
    return inputData;
  }

  public void setInputData(Map<InputDataType, List<String>> inputData) {
    this.inputData = inputData;
  }

  public OAIPMHHarvestingDetails getHarvestingDetails() {
    return harvestingDetails;
  }

  public void setHarvestingDetails(OAIPMHHarvestingDetails harvestingDetails) {
    this.harvestingDetails = harvestingDetails;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    var dpsTask = (DpsTask) o;
    return taskId == dpsTask.taskId &&
        com.google.common.base.Objects.equal(inputData, dpsTask.inputData) &&
        Objects.equal(parameters, dpsTask.parameters) &&
        Objects.equal(outputRevision, dpsTask.outputRevision) &&
        Objects.equal(taskName, dpsTask.taskName) &&
        Objects.equal(harvestingDetails, dpsTask.harvestingDetails);
  }

  public String toJSON() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public static DpsTask fromJSON(String json) throws IOException {
    return new ObjectMapper().readValue(json, DpsTask.class);
  }

  public static DpsTask fromTaskInfo(TaskInfo taskInfo) throws IOException {
    return fromJSON(taskInfo.getDefinition());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(inputData, parameters, outputRevision, taskId, taskName, harvestingDetails);
  }
}

