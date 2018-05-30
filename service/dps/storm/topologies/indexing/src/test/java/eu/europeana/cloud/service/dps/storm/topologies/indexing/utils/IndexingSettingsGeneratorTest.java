package eu.europeana.cloud.service.dps.storm.topologies.indexing.utils;

import com.mongodb.ServerAddress;
import eu.europeana.indexing.exception.IndexerConfigurationException;
import eu.europeana.indexing.IndexingSettings;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class IndexingSettingsGeneratorTest {

    @Test
    public void shouldGenerateProperSettingsForPreviewDB() throws IOException, IndexerConfigurationException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPreview();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPublishDB() throws IOException, IndexerConfigurationException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPublish();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPreviewDBAndTestEnv() throws IOException, IndexerConfigurationException, URISyntaxException {
        Properties prop = loadProperties("indexing_with_env_prefix.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator("test", prop);
        IndexingSettings settings = generator.generateForPreview();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty("test." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty("test." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty("test." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty("test." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPublishDBAndTestEnv() throws IOException, IndexerConfigurationException, URISyntaxException {
        Properties prop = loadProperties("indexing_with_env_prefix.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator("test", prop);
        IndexingSettings settings = generator.generateForPublish();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty("test." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty("test." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty("test." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty("test." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    private Properties loadProperties(String fileName) throws IOException {

        InputStream input = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(fileName);

        Properties prop = new Properties();
        prop.load(input);
        return prop;
    }
}