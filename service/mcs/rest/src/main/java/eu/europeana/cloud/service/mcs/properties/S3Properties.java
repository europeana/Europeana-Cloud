package eu.europeana.cloud.service.mcs.properties;

import lombok.Getter;
import lombok.Setter;

/**
 * Class that holds properties related with connection to S3
 * All fields are required beside partSize which has a default value of 15MB.
 */
@Setter
@Getter
public class S3Properties {
  private static final int DEFAULT_MAX_PART_SIZE = 15 * 1024 * 1024;

  private String container;
  private String endpoint;
  private String user;
  private String password;
  private String region;
  private int maxPartSize = DEFAULT_MAX_PART_SIZE;
}
