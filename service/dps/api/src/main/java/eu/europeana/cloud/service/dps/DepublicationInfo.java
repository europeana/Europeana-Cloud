package eu.europeana.cloud.service.dps;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of the depublication task source input
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DepublicationInfo implements TaskSource {
  private boolean depublishWholeDataset;
  private Set<String> europeanaIdsToDepublish;
}
