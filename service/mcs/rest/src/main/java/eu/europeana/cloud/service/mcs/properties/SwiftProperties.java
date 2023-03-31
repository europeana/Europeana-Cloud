package eu.europeana.cloud.service.mcs.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Setter
@Getter
@ConfigurationProperties(prefix = "swift")
public class SwiftProperties {
  private String provider;
  private String container;
  private String endpoint;
  private String user;
  private String password;
}
