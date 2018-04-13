package eu.europeana.cloud.service.dps.storm.topologies.indexing.utils;

import com.mongodb.ServerAddress;
import eu.europeana.indexing.IndexerConfigurationException;
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
    public void shouldGenerateProperSettingsForPreviewEnv() throws IOException, IndexerConfigurationException, URISyntaxException {
        //
        InputStream input = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream("indexing.properties");

        // load a properties file
        Properties prop = new Properties();
        prop.load(input);
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPreview();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME).toString());
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).toString().contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPublishEnv() throws IOException, IndexerConfigurationException, URISyntaxException {
        //
        InputStream input = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream("indexing.properties");

        // load a properties file
        Properties prop = new Properties();
        prop.load(input);
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPublish();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME).toString());
        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).toString().contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }
}