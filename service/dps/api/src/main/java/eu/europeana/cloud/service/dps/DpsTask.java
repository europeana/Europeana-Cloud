package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import java.io.IOException;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@XmlRootElement
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DpsTask implements Serializable {

  private static final long serialVersionUID = 1L;

  private TaskInput input;
  private TaskOutput output;

  /* List of parameters (specific for each dps-topology) */
  private Map<String, String> parameters = new HashMap<>();

  /* Unique id for this task */
  private long taskId;

  /* Name for the task */
  private String taskName;

  public DpsTask(String taskName) {
  }

  public void addParameter(String parameterKey, String parameterValue) {
    parameters.put(parameterKey, parameterValue);
  }

  public String getParameter(String parameterKey) {
    return parameters.get(parameterKey);
  }

  public void removeParameter(String name) {
    parameters.remove(name);
  }

  public String toJSON() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public static DpsTask fromJSON(String json) throws IOException {
    if (json == null) {
      throw new IllegalArgumentException("Task definition json is null");
    }
    return new ObjectMapper().readValue(json, DpsTask.class);
  }

  public static DpsTask fromTaskInfo(TaskInfo taskInfo) throws IOException {
    return fromJSON(taskInfo.getDefinition());
  }

  @JsonTypeInfo(use= Id.NAME)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = OAIPMHHarvestingDetails.class,name="oai"),
      @JsonSubTypes.Type(value = HttpHarvestingDetails.class,name="http"),
      @JsonSubTypes.Type(value = FilesUrls.class,name="files"),
      @JsonSubTypes.Type(value = DatasetRevisionInfo.class,name="revision"),
      @JsonSubTypes.Type(value = BatchInfo.class,name="batch")
  })
  public interface TaskInput {

  }


  @JsonTypeInfo(use= Id.NAME)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = DatasetRevisionInfo.class,name="revision"),
      @JsonSubTypes.Type(value = BatchInfo.class,name="batch")
  })
  public interface TaskOutput extends MCSInputOutput {

  }

}

