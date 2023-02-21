package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.TaskState;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@ToString
@Getter
@AllArgsConstructor
public class HarvestResult {

  private int resultCounter;
  private TaskState taskState;

}
