package eu.europeana.cloud.service.dps.service.utils.indexing;


import com.mongodb.ServerAddress;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexingSettingsGeneratorTest {

  @Test
  void shouldGenerateProperSettingsForPreviewDB() throws IOException, IndexingException, URISyntaxException {
    String previewPrefixWithDelimiter = IndexingPropertyNames.PREVIEW_PREFIX + IndexingPropertyNames.DELIMITER;
    Properties prop = loadProperties();

    IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
    IndexingSettings settings = generator.generateForPreview();
    assertEquals(settings.getMongoDatabaseName(),
            prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_DB_NAME));
    assertEquals(settings.getRecordRedirectDatabaseName(),
            prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_REDIRECTS_DB_NAME));
    assertEquals(settings.getMongoProperties().getApplicationName(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_APPLICATION_NAME));
    assertEquals(settings.getMongoProperties().getMaxConnectionPoolSize().intValue(),
        Integer.parseInt(prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_POOL_SIZE)));

    List<ServerAddress> mongos = settings.getMongoProperties().getMongoHosts();
    for (ServerAddress mongo : mongos) {
      assertTrue(
          prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_INSTANCES).contains(mongo.getHost()));
      assertEquals(String.valueOf(mongo.getPort()),
          prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.MONGO_PORT_NUMBER));
    }
    assertEquals(settings.getSolrProperties().getZookeeperHosts().size(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingPropertyNames.ZOOKEEPER_INSTANCES).split(",").length);
    assertFalse(settings.getSolrProperties().getSolrUseHttp1());
  }

  @Test
  void shouldGenerateProperSettingsForPublishDB() throws IOException, IndexingException, URISyntaxException {
    String publishPrefixWithDelimiter = IndexingPropertyNames.PUBLISH_PREFIX + IndexingPropertyNames.DELIMITER;
    Properties prop = loadProperties();

    IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
    IndexingSettings settings = generator.generateForPublish();
    assertEquals(settings.getMongoDatabaseName(),
            prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_DB_NAME));
    assertEquals(settings.getRecordRedirectDatabaseName(),
            prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_REDIRECTS_DB_NAME));
    assertEquals(settings.getMongoProperties().getApplicationName(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_APPLICATION_NAME));
    assertEquals(settings.getMongoProperties().getMaxConnectionPoolSize().intValue(),
        Integer.parseInt(prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_POOL_SIZE)));

    List<ServerAddress> mongos = settings.getMongoProperties().getMongoHosts();
    for (ServerAddress mongo : mongos) {
      assertTrue(
          prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_INSTANCES).contains(mongo.getHost()));
      assertEquals(String.valueOf(mongo.getPort()),
          prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.MONGO_PORT_NUMBER));
    }
    assertEquals(settings.getSolrProperties().getZookeeperHosts().size(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingPropertyNames.ZOOKEEPER_INSTANCES).split(",").length);
    assertTrue(settings.getSolrProperties().getSolrUseHttp1());
  }

  private Properties loadProperties() throws IOException {

    InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("indexing.properties");

    Properties prop = new Properties();
    prop.load(input);
    return prop;
  }
}