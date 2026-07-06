package eu.europeana.cloud.service.dps.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.TaskInput;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of the task parameters which is sent to DPS service.
 */
@XmlRootElement
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DpsTask {

  private TaskInput input;
  private BatchInfo output;

  /* List of parameters (specific for each dps-topology) */
  private Map<String, String> parameters = new HashMap<>();

  /* Unique id for this task */
  private long taskId;

  /* Name for the task */
  private String taskName;

  /**
   * Constructor
   * @param taskName - task name
   */
  public DpsTask(String taskName) {
    this.taskName = taskName;
  }


  /**
   * Adds task general parameter
   * @param parameterKey  - key of the parameter
   * @param parameterValue - value of the parameter
   */
  public void addParameter(String parameterKey, String parameterValue) {
    parameters.put(parameterKey, parameterValue);
  }

  /**
   * Adds task general parameter
   * @param parameterKey - key of the parameter
   * @return parameter value
   */
  public String getParameter(String parameterKey) {
    return parameters.get(parameterKey);
  }

  /**
   * Removes task general parameter
   * @param name - key of the parameter
   */
  public void removeParameter(String name) {
    parameters.remove(name);
  }

  /**
   * @return string representation of the DpsTask in JSON format
   * @throws JsonProcessingException when some of the parameters could not be serialized as JSON
   */
  public String toJSON() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }

  /**
   * Deserializes task
   * @param json - string containing the DpsTask in JSON format
   * @return DPS task instance
   * @throws JsonProcessingException when string could not be properly parsed as DpsTask
   */
  public static DpsTask fromJSON(String json) throws JsonProcessingException {
    if (json == null) {
      throw new IllegalArgumentException("Task definition json is null");
    }
    return new ObjectMapper().readValue(json, DpsTask.class);
  }

  /**
   * Deserializes task
   * @param taskInfo - task database entity
   * @return DPS task instance
   * @throws JsonProcessingException when task definition stored in entity could not be properly parsed as DpsTask
   */
  public static DpsTask fromTaskInfo(TaskInfo taskInfo) throws JsonProcessingException {
    return fromJSON(taskInfo.getDefinition());
  }

}

