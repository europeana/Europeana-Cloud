package eu.europeana.cloud.service.dps;


import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsTask.TaskInput;
import eu.europeana.cloud.service.dps.DpsTask.TaskOutput;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of the DpsTask dataset revision input or output
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DatasetRevisionInfo implements TaskInput, TaskOutput {

  private String providerId;
  private String datasetId;
  private String representationName;
  private Revision revision;
}
