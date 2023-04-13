package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

  private String topologyAvailableTopics;
  private String brokerLocation;
  private String groupId;

}
