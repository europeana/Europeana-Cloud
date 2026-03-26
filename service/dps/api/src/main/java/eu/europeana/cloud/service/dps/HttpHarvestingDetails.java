package eu.europeana.cloud.service.dps;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpHarvestingDetails implements DpsTask.TaskInput {

  private String repositoryUrl;

}
