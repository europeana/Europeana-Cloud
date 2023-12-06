package eu.europeana.cloud.service.dps.service.utils.indexing;

public final class IndexingPropertyNames {

  public static final String PREVIEW_PREFIX = "indexing.preview";
  public static final String PUBLISH_PREFIX = "indexing.publish";
  public static final String MONGO_INSTANCES = "mongoInstances";
  public static final String MONGO_PORT_NUMBER = "mongoPortNumber";
  public static final String MONGO_DB_NAME = "mongoDbName";
  public static final String MONGO_REDIRECTS_DB_NAME = "mongoRedirectsDbName";
  public static final String MONGO_USERNAME = "mongoUsername";
  public static final String MONGO_SECRET = "mongoPassword";
  public static final String MONGO_USE_SSL = "mongoUseSSL";
  public static final String MONGO_READ_PREFERENCE = "mongoReadPreference";
  public static final String MONGO_APPLICATION_NAME = "mongoApplicationName";
  public static final String MONGO_POOL_SIZE = "mongoPoolSize";
  public static final String MONGO_AUTH_DB = "mongoAuthDB";
  //
  public static final String SOLR_INSTANCES = "solrInstances";
  //
  public static final String ZOOKEEPER_INSTANCES = "zookeeperInstances";
  public static final String ZOOKEEPER_PORT_NUMBER = "zookeeperPortNumber";
  public static final String ZOOKEEPER_CHROOT = "zookeeperChroot";
  public static final String ZOOKEEPER_DEFAULT_COLLECTION = "zookeeperDefaultCollection";
  public static final String DELIMITER = ".";

  private IndexingPropertyNames() {
  }

}
