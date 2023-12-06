package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Holds the data related with dataset as it is stored in Metis
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetisDataset {

  private String id;
  private long size;
}
