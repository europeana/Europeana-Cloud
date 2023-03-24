package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "cassandra")
public class CassandraProperties {

  private String AASKeyspace;
  private String AASUser;
  private String AASPassword;
  private String DPSKeyspace;
  private String DPSUser;
  private String DPSPassword;
  private String MCSKeyspace;
  private String MCSUser;
  private String MCSPassword;
  private String hosts;
  private String port;

}
