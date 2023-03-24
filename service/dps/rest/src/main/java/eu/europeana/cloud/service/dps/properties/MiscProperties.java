package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "misc")
public class MiscProperties {

  private String appId;
  private String machineLocation;
  private String mcsLocation;
  private String uisLocation;
}
