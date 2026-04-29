package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.service.dps.DpsTask.TaskInput;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Definition of the http harvesting DpsTask input
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpHarvestingDetails implements TaskInput {

  private String repositoryUrl;

}
