package eu.europeana.cloud.service.mcs.properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CassandraProperties {

  private String Keyspace;
  private String User;
  private String Password;
  private String hosts;
  private int port;
}
