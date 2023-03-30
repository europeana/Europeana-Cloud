package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CassandraProperties {

  private String User;
  private String Password;
  private String Keyspace;
  private String hosts;
  private int port;

}
