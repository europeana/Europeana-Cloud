package eu.europeana.cloud.service.mcs.properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CassandraProperties {

  private String keyspace;
  private String user;
  private String password;
  private String hosts;
  private int port;
}
