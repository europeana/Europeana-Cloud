package eu.europeana.cloud.service.dps.storm.utils;

import java.io.Serializable;
import lombok.Builder;
import lombok.Getter;

/**
 * Encapsulates information need for database connection;
 */
@Builder
@Getter
public class DbConnectionDetails implements Serializable {

  private final int port;
  private final String keyspaceName;
  private final String userName;
  private final String password;
  private final String hosts;
}
