package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "general")
public class GeneralProperties {

  private String appId;
  private String machineLocation;
  private String mcsLocation;
  private String uisLocation;
  private String contextPath;
}
