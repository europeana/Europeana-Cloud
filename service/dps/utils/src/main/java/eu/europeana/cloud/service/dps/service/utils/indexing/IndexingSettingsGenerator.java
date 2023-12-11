package eu.europeana.cloud.service.dps.service.utils.indexing;

import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper.IndexingType;
import eu.europeana.cloud.common.properties.IndexingProperties;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pwozniak on 4/11/18
 */
public class IndexingSettingsGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingSettingsGenerator.class);

  protected final IndexingProperties previewIndexingProperties;
  protected final IndexingProperties publishIndexingProperties;

  public IndexingSettingsGenerator(IndexingProperties previewIndexingProperties, IndexingProperties publishIndexingProperties) {
    this.previewIndexingProperties = previewIndexingProperties;
    this.publishIndexingProperties = publishIndexingProperties;
  }

  public IndexingSettingsGenerator(Properties indexingPropertyFile) {
    this.previewIndexingProperties
        = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(indexingPropertyFile, IndexingType.PREVIEW);
    this.publishIndexingProperties
        = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(indexingPropertyFile, IndexingType.PUBLISH);
  }

  public IndexingSettings generateForPreview() throws IndexingException, URISyntaxException {
    if (isNotDefinedFor(previewIndexingProperties)) {
      return null;
    }
    IndexingSettings indexingSettings = new IndexingSettings();
    prepareSettingFor(previewIndexingProperties, indexingSettings);
    return indexingSettings;
  }

  public IndexingSettings generateForPublish() throws IndexingException, URISyntaxException {
    if (isNotDefinedFor(publishIndexingProperties)) {
      return null;
    }
    IndexingSettings indexingSettings = new IndexingSettings();
    prepareSettingFor(publishIndexingProperties, indexingSettings);
    return indexingSettings;
  }

  private boolean isNotDefinedFor(IndexingProperties indexingProperties) {
    return indexingProperties.getMongoInstances() == null;
  }

  private void prepareSettingFor(IndexingProperties indexingProperties, IndexingSettings indexingSettings)
      throws IndexingException, URISyntaxException {
    prepareMongoSettings(indexingSettings, indexingProperties);
    prepareSolrSetting(indexingSettings, indexingProperties);
    prepareZookeeperSettings(indexingSettings, indexingProperties);
  }

  private void prepareMongoSettings(IndexingSettings indexingSettings, IndexingProperties indexingProperties)
      throws IndexingException {
    String mongoInstances = indexingProperties.getMongoInstances();
    int mongoPort = indexingProperties.getMongoPortNumber();
    String[] instances = mongoInstances.trim().split(",");
    for (String instance : instances) {
      indexingSettings.addMongoHost(new InetSocketAddress(instance, mongoPort));
    }
    indexingSettings
        .setMongoDatabaseName(indexingProperties.getMongoDbName());
    indexingSettings.setRecordRedirectDatabaseName(indexingProperties.getMongoRedirectsDbName());

    if (mongoCredentialsProvidedFor(indexingProperties)) {
      indexingSettings.setMongoCredentials(
          indexingProperties.getMongoUsername(),
          indexingProperties.getMongoPassword(),
          indexingProperties.getMongoAuthDb());
    } else {
      LOGGER.info("Mongo credentials not provided");
    }

    Optional<Integer> optionalMongoPoolSize = Optional.ofNullable(indexingProperties.getMongoPoolSize());
    optionalMongoPoolSize.ifPresentOrElse(
        indexingSettings::setMongoMaxConnectionPoolSize,
        () -> LOGGER.warn("Mongo max connection pool size not provided"));

    if (indexingProperties.getMongoUseSSL() != null && indexingProperties.getMongoUseSSL().equalsIgnoreCase("true")) {
      indexingSettings.setMongoEnableSsl();
    }
    indexingSettings
        .setMongoReadPreference(indexingProperties.getMongoReadPreference());
    indexingSettings.setMongoApplicationName(indexingProperties.getMongoApplicationName());

  }

  private boolean mongoCredentialsProvidedFor(IndexingProperties indexingProperties) {
    return !"".equals(indexingProperties.getMongoUsername()) &&
        !"".equals(indexingProperties.getMongoPassword()) &&
        !"".equals(indexingProperties.getMongoAuthDb());
  }

  private void prepareSolrSetting(IndexingSettings indexingSettings, IndexingProperties indexingProperties)
      throws URISyntaxException, IndexingException {
    String solrInstances = indexingProperties.getSolrInstances();
    String[] instances = solrInstances.trim().split(",");
    for (String instance : instances) {
      indexingSettings.addSolrHost(new URI(instance));
    }
  }

  private void prepareZookeeperSettings(IndexingSettings indexingSettings, IndexingProperties indexingProperties)
      throws IndexingException {
    String zookeeperInstances = indexingProperties.getZookeeperInstances();
    int zookeeperPort = indexingProperties.getZookeeperPortNumber();
    String[] instances = zookeeperInstances.trim().split(",");
    for (String instance : instances) {
      indexingSettings.addZookeeperHost(new InetSocketAddress(instance, zookeeperPort));
    }
    indexingSettings
        .setZookeeperChroot(indexingProperties.getZookeeperChroot());
    indexingSettings.setZookeeperDefaultCollection(indexingProperties.getZookeeperDefaultCollection());
  }
}
