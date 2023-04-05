package eu.europeana.cloud.service.mcs.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Setter
@Getter
@ConfigurationProperties(prefix = "general")
public class GeneralProperties {

  private String uisLocation;
}
