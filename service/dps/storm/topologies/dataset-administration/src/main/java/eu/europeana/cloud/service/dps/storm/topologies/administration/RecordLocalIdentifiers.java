package eu.europeana.cloud.service.dps.storm.topologies.administration;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class RecordLocalIdentifiers {

  private String primary;
  private List<String> secondary;
}
