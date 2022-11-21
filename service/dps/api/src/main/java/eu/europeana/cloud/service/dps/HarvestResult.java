package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.TaskState;
import lombok.*;

@Builder
@ToString
@Getter
@AllArgsConstructor
public class HarvestResult {

  private int resultCounter;
  private TaskState taskState;

}
