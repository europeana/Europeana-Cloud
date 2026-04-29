package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import eu.europeana.cloud.service.dps.DpsTask.TaskInput;
import eu.europeana.cloud.service.dps.DpsTask.TaskOutput;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of the DpsTask batch input or output
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchInfo implements TaskInput, TaskOutput {

  private String providerId;
  private String batchId;   //It internally in eCloud resolves into dedicated datasetId
  private String representationName;

  @JsonIgnore
  @Override
  public String getDatasetId() {
    return batchId;
  }
}
