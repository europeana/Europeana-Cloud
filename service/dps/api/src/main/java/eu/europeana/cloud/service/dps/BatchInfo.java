package eu.europeana.cloud.service.dps;

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
public class BatchInfo implements TaskSource {

  private String providerId;
  private String batchId;   //It internally in eCloud resolves into dedicated datasetId

}
