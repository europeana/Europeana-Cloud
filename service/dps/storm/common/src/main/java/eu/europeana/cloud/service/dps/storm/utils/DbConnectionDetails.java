package eu.europeana.cloud.service.dps.storm.utils;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

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
