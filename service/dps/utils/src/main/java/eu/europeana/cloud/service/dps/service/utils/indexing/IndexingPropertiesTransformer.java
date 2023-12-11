package eu.europeana.cloud.service.dps.service.utils.indexing;

import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.DELIMITER;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_APPLICATION_NAME;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_AUTH_DB;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_DB_NAME;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_INSTANCES;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_POOL_SIZE;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_PORT_NUMBER;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_READ_PREFERENCE;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_REDIRECTS_DB_NAME;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_SECRET;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_USERNAME;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.MONGO_USE_SSL;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.PREVIEW_PREFIX;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.PUBLISH_PREFIX;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.SOLR_INSTANCES;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.ZOOKEEPER_CHROOT;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.ZOOKEEPER_DEFAULT_COLLECTION;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.ZOOKEEPER_INSTANCES;
import static eu.europeana.cloud.service.dps.service.utils.indexing.IndexingPropertyNames.ZOOKEEPER_PORT_NUMBER;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper.IndexingType;
import eu.europeana.cloud.common.properties.IndexingProperties;
import java.util.Properties;

public class IndexingPropertiesTransformer {

  public static IndexingProperties getIndexingPropertiesFromPropertyFile(Properties properties, IndexingType type) {
    String prefix = "";
    switch (type) {
      case PUBLISH:
        prefix = PUBLISH_PREFIX + DELIMITER;
        break;
      case PREVIEW:
        prefix = PREVIEW_PREFIX + DELIMITER;
        break;
    }
    IndexingProperties indexingProperties = new IndexingProperties();
    indexingProperties.setMongoDbName(properties.getProperty(prefix + MONGO_DB_NAME));
    indexingProperties.setMongoUsername(properties.getProperty(prefix + MONGO_USERNAME));
    indexingProperties.setMongoPassword(properties.getProperty(prefix + MONGO_SECRET));
    indexingProperties.setMongoApplicationName(properties.getProperty(prefix + MONGO_APPLICATION_NAME));
    indexingProperties.setMongoInstances(properties.getProperty(prefix + MONGO_INSTANCES));
    indexingProperties.setMongoPoolSize(Integer.parseInt(properties.getProperty(prefix + MONGO_POOL_SIZE)));
    indexingProperties.setMongoRedirectsDbName(properties.getProperty(prefix + MONGO_REDIRECTS_DB_NAME));
    indexingProperties.setMongoPortNumber(Integer.parseInt(properties.getProperty(prefix + MONGO_PORT_NUMBER)));
    indexingProperties.setMongoAuthDb(properties.getProperty(prefix + MONGO_AUTH_DB));
    indexingProperties.setMongoUseSSL(properties.getProperty(prefix + MONGO_USE_SSL));
    indexingProperties.setMongoReadPreference(properties.getProperty(prefix + MONGO_READ_PREFERENCE));

    indexingProperties.setSolrInstances(properties.getProperty(prefix + SOLR_INSTANCES));

    indexingProperties.setZookeeperChroot(properties.getProperty(prefix + ZOOKEEPER_CHROOT));
    indexingProperties.setZookeeperInstances(properties.getProperty(prefix + ZOOKEEPER_INSTANCES));
    indexingProperties.setZookeeperDefaultCollection(properties.getProperty(prefix + ZOOKEEPER_DEFAULT_COLLECTION));
    indexingProperties.setZookeeperPortNumber(Integer.parseInt(properties.getProperty(prefix + ZOOKEEPER_PORT_NUMBER)));
    return indexingProperties;
  }
}
