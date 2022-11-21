package eu.europeana.cloud.service.dps.service.utils.indexing;

import com.mongodb.ServerAddress;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexingSettingsGeneratorTest {

  @Test
  public void shouldGenerateProperSettingsForPreviewDB() throws IOException, IndexingException, URISyntaxException {
    String previewPrefixWithDelimiter = IndexingSettingsGenerator.PREVIEW_PREFIX + IndexingSettingsGenerator.DELIMITER;
    Properties prop = loadProperties();

    IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
    IndexingSettings settings = generator.generateForPreview();
    assertEquals(settings.getMongoDatabaseName(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_DB_NAME));
    assertEquals(settings.getRecordRedirectDatabaseName(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
    assertEquals(settings.getMongoProperties().getApplicationName(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));
    assertEquals(settings.getMongoProperties().getMaxConnectionPoolSize().intValue(),
        Integer.parseInt(prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_POOL_SIZE)));

    List<ServerAddress> mongos = settings.getMongoProperties().getMongoHosts();
    for (ServerAddress mongo : mongos) {
      assertTrue(
          prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
      assertEquals(String.valueOf(mongo.getPort()),
          prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_PORT_NUMBER));
    }
    assertEquals(settings.getSolrProperties().getZookeeperHosts().size(),
        prop.getProperty(previewPrefixWithDelimiter + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
  }

  @Test
  public void shouldGenerateProperSettingsForPublishDB() throws IOException, IndexingException, URISyntaxException {
    String publishPrefixWithDelimiter = IndexingSettingsGenerator.PUBLISH_PREFIX + IndexingSettingsGenerator.DELIMITER;
    Properties prop = loadProperties();

    IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
    IndexingSettings settings = generator.generateForPublish();
    assertEquals(settings.getMongoDatabaseName(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_DB_NAME));
    assertEquals(settings.getRecordRedirectDatabaseName(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
    assertEquals(settings.getMongoProperties().getApplicationName(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));
    assertEquals(settings.getMongoProperties().getMaxConnectionPoolSize().intValue(),
        Integer.parseInt(prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_POOL_SIZE)));

    List<ServerAddress> mongos = settings.getMongoProperties().getMongoHosts();
    for (ServerAddress mongo : mongos) {
      assertTrue(
          prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
      assertEquals(String.valueOf(mongo.getPort()),
          prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.MONGO_PORT_NUMBER));
    }
    assertEquals(settings.getSolrProperties().getZookeeperHosts().size(),
        prop.getProperty(publishPrefixWithDelimiter + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
  }

  private Properties loadProperties() throws IOException {

    InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("indexing.properties");

    Properties prop = new Properties();
    prop.load(input);
    return prop;
  }
}