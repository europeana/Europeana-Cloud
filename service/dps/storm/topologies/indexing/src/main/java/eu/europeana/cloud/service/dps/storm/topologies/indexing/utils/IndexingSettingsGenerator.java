package eu.europeana.cloud.service.dps.storm.topologies.indexing.utils;

import eu.europeana.indexing.IndexerConfigurationException;
import eu.europeana.indexing.IndexingSettings;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by pwozniak on 4/11/18
 */
public class IndexingSettingsGenerator {

    public static final String PREVIEW_PREFIX = "preview";
    public static final String PUBLISH_PREFIX = "publish";

    public static final String MONGO_INSTANCES = "mongo.instances";
    public static final String MONGO_PORT_NUMBER = "mongo.portNumber";
    public static final String MONGO_DB_NAME = "mongo.dbName";
    public static final String MONGO_USERNAME = "mongo.username";
    public static final String MONGO_PASSWORD = "mongo.password";
    public static final String MONGO_USE_SSL = "mongo.useSSL";
    public static final String MONGO_AUTH_DB = "mongo.authDB";
    //
    public static final String SOLR_INSTANCES = "solr.instances";
    //
    public static final String ZOOKEEPER_INSTANCES = "zookeeper.instances";
    public static final String ZOOKEEPER_PORT_NUMBER = "zookeeper.portNumber";
    public static final String ZOOKEEPER_CHROOT = "zookeeper.chroot";
    public static final String ZOOKEEPER_DEFAULT_COLLECTION = "zookeeper.defaultCollection";

    public static void main(String[] args) throws IOException, IndexerConfigurationException, URISyntaxException {
        //
        InputStream input = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream("indexing.properties");

        // load a properties file
        Properties prop = new Properties();
        prop.load(input);
        //
        IndexingSettingsGenerator g = new IndexingSettingsGenerator();
        IndexingSettings s1 = g.generateForPreview(prop);
        IndexingSettings s2 = g.generateForPublish(prop);
        System.out.println("sample");
    }


    public IndexingSettings generateForPreview(Properties properties) throws IndexerConfigurationException, URISyntaxException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareSettingFor(PREVIEW_PREFIX, indexingSettings, properties);
        return indexingSettings;
    }

    public IndexingSettings generateForPublish(Properties properties) throws IndexerConfigurationException, URISyntaxException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareSettingFor(PUBLISH_PREFIX, indexingSettings, properties);
        return indexingSettings;
    }

    private void prepareSettingFor(String environment, IndexingSettings indexingSettings, Properties properties) throws IndexerConfigurationException, URISyntaxException {
        prepareMongoSettings(indexingSettings, properties, environment);
        prepareSolrSetting(indexingSettings, properties, environment);
        prepareZookeeperSettings(indexingSettings, properties, environment);
    }

    private void prepareMongoSettings(IndexingSettings indexingSettings, Properties properties, String prefix) throws IndexerConfigurationException {
        String mongoInstances = properties.get(prefix + "." + MONGO_INSTANCES).toString();
        int mongoPort = Integer.parseInt(properties.get(prefix + "." + MONGO_PORT_NUMBER).toString());
        String[] instances = mongoInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addMongoHost(new InetSocketAddress(instance, mongoPort));
        }
        indexingSettings.setMongoDatabaseName(properties.get(prefix + "." + MONGO_DB_NAME).toString());
        indexingSettings.setMongoCredentials(
                properties.get(prefix + "." + MONGO_USERNAME).toString(),
                properties.get(prefix + "." + MONGO_PASSWORD).toString(),
                properties.get(prefix + "." + MONGO_AUTH_DB).toString());
        if (properties.getProperty(prefix + "." + MONGO_USE_SSL) != null && properties.getProperty(prefix + "." + MONGO_USE_SSL).equalsIgnoreCase("true")) {
            indexingSettings.setMongoEnableSsl();
        }
    }

    private void prepareSolrSetting(IndexingSettings indexingSettings, Properties properties, String prefix) throws URISyntaxException, IndexerConfigurationException {
        String solrInstances = properties.get(prefix + "." + SOLR_INSTANCES).toString();
        String[] instances = solrInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addSolrHost(new URI(instance));
        }
    }

    private void prepareZookeeperSettings(IndexingSettings indexingSettings, Properties properties, String prefix) throws IndexerConfigurationException {
        String zookeeperInstances = properties.get(prefix + "." + ZOOKEEPER_INSTANCES).toString();
        int zookeeperPort = Integer.parseInt(properties.get(prefix + "." + ZOOKEEPER_PORT_NUMBER).toString());
        String[] instances = zookeeperInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addZookeeperHost(new InetSocketAddress(instance, zookeeperPort));
        }
        indexingSettings.setZookeeperChroot(properties.getProperty(prefix + "." + ZOOKEEPER_CHROOT));
        indexingSettings.setZookeeperDefaultCollection(properties.getProperty(prefix + "." + ZOOKEEPER_DEFAULT_COLLECTION));
    }
}
