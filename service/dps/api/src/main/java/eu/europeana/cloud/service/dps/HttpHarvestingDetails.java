package eu.europeana.cloud.service.dps;

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
