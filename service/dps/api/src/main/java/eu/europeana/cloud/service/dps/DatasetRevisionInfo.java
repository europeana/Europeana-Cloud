package eu.europeana.cloud.service.dps;


import eu.europeana.cloud.common.model.Revision;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DatasetRevisionInfo implements MCSInputOutput, DpsTask.TaskInput, DpsTask.TaskOutput {
  private String providerId;
  private String datasetId;
  private String representationName;
  private Revision revision;
}
