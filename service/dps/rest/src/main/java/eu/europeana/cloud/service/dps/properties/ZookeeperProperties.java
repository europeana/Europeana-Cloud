package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "zookeeper")
public class ZookeeperProperties {

  private String address;
}
