package eu.europeana.cloud.service.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BatchInfo implements MCSInputOutput, DpsTask.TaskInput, DpsTask.TaskOutput {
  private String providerId;
  private String batchId;   //It internally in eCloud resolves into dedicated datasetId
  private String representationName;

  @Override
  public String getDatasetId() {
    return batchId;
  }
}
