package eu.europeana.cloud.service.dps.service.utils.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.mongodb.ServerAddress;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import org.junit.Test;

public class IndexingSettingsGeneratorTest {

  @Test
  public void shouldGenerateProperSettingsForPreviewDB() throws IOException, IndexingException, URISyntaxException {
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
  }

  @Test
  public void shouldGenerateProperSettingsForPublishDB() throws IOException, IndexingException, URISyntaxException {
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
  }

  private Properties loadProperties() throws IOException {

    InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("indexing.properties");

    Properties prop = new Properties();
    prop.load(input);
    return prop;
  }
}