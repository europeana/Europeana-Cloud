package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "topology")
public class TopologyProperties {
  private String user;
  private String password;
  private String nameList;
}
