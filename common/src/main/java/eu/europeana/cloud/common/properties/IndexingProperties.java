package eu.europeana.cloud.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class IndexingProperties {

  private String mongoInstances;
  private int mongoPortNumber;
  private String mongoDbName;
  private String mongoRedirectsDbName;
  private String mongoUsername;
  private String mongoPassword;
  private String mongoAuthDb;
  private String mongoUseSSL;
  private String mongoReadPreference;
  private String mongoApplicationName;
  private int mongoPoolSize;
  private String mongoTombstoneDbName;

  private String solrInstances;

  private String zookeeperInstances;
  private int zookeeperPortNumber;
  private String zookeeperChroot;
  private String zookeeperDefaultCollection;
}
