package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaProperties {

  private String topologyAvailableTopics;
  private String brokerLocation;
  private String groupId;

}
