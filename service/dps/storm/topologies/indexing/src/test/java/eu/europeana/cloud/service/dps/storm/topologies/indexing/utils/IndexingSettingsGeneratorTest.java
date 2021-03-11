package eu.europeana.cloud.service.dps.storm.topologies.indexing.utils;

import com.mongodb.ServerAddress;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class IndexingSettingsGeneratorTest {

    @Test
    public void shouldGenerateProperSettingsForPreviewDB() throws IOException, IndexingException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPreview();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        assertEquals(settings.getRecordRedirectDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
        assertEquals(settings.getMongoProperties().getApplicationName(), prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));

        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPublishDB() throws IOException, IndexingException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(prop);
        IndexingSettings settings = generator.generateForPublish();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        assertEquals(settings.getRecordRedirectDatabaseName(), prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
        assertEquals(settings.getMongoProperties().getApplicationName(), prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));

        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPreviewDBAndAnotherEnv() throws IOException, IndexingException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, prop);
        IndexingSettings settings = generator.generateForPreview();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        assertEquals(settings.getRecordRedirectDatabaseName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
        assertEquals(settings.getMongoProperties().getApplicationName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));

        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PREVIEW_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    @Test
    public void shouldGenerateProperSettingsForPublishDBAndAnotherEnv() throws IOException, IndexingException, URISyntaxException {
        Properties prop = loadProperties("indexing.properties");
        //
        IndexingSettingsGenerator generator = new IndexingSettingsGenerator(TargetIndexingEnvironment.ALTERNATIVE, prop);
        IndexingSettings settings = generator.generateForPublish();
        assertEquals(settings.getMongoDatabaseName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_DB_NAME));
        assertEquals(settings.getRecordRedirectDatabaseName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_REDIRECTS_DB_NAME));
        assertEquals(settings.getMongoProperties().getApplicationName(), prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_APPLICATION_NAME));

        List<ServerAddress> mongos = settings.getMongoHosts();
        for (ServerAddress mongo : mongos) {
            assertTrue(prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_INSTANCES).contains(mongo.getHost()));
            assertTrue(prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.MONGO_PORT_NUMBER).equals(mongo.getPort() + ""));
        }
        assertTrue(settings.getZookeeperHosts().size() == prop.getProperty(TargetIndexingEnvironment.ALTERNATIVE + "." + IndexingSettingsGenerator.PUBLISH_PREFIX + "." + IndexingSettingsGenerator.ZOOKEEPER_INSTANCES).split(",").length);
    }

    private Properties loadProperties(String fileName) throws IOException {

        InputStream input = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(fileName);

        Properties prop = new Properties();
        prop.load(input);
        return prop;
    }
}