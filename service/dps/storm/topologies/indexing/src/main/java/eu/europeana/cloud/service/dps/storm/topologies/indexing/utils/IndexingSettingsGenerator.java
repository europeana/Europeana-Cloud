package eu.europeana.cloud.service.dps.storm.topologies.indexing.utils;

import eu.europeana.indexing.IndexerConfigurationException;
import eu.europeana.indexing.IndexingSettings;

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

    public static final String DELIMITER = ".";

    private Properties properties;

    public IndexingSettingsGenerator(Properties properties) {
        this.properties = properties;
    }

    public IndexingSettings generateForPreview() throws IndexerConfigurationException, URISyntaxException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareSettingFor(PREVIEW_PREFIX, indexingSettings);
        return indexingSettings;
    }

    public IndexingSettings generateForPublish() throws IndexerConfigurationException, URISyntaxException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareSettingFor(PUBLISH_PREFIX, indexingSettings);
        return indexingSettings;
    }

    private void prepareSettingFor(String environment, IndexingSettings indexingSettings) throws IndexerConfigurationException, URISyntaxException {
        prepareMongoSettings(indexingSettings, environment);
        prepareSolrSetting(indexingSettings, environment);
        prepareZookeeperSettings(indexingSettings, environment);
    }

    private void prepareMongoSettings(IndexingSettings indexingSettings, String prefix) throws IndexerConfigurationException {
        String mongoInstances = properties.get(prefix + DELIMITER + MONGO_INSTANCES).toString();
        int mongoPort = Integer.parseInt(properties.get(prefix + DELIMITER + MONGO_PORT_NUMBER).toString());
        String[] instances = mongoInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addMongoHost(new InetSocketAddress(instance, mongoPort));
        }
        indexingSettings.setMongoDatabaseName(properties.get(prefix + DELIMITER + MONGO_DB_NAME).toString());

        if (mongoCredentialsProvidedFor(prefix)) {
            indexingSettings.setMongoCredentials(
                    properties.get(prefix + DELIMITER + MONGO_USERNAME).toString(),
                    properties.get(prefix + DELIMITER + MONGO_PASSWORD).toString(),
                    properties.get(prefix + DELIMITER + MONGO_AUTH_DB).toString());
        }

        if (properties.getProperty(prefix + DELIMITER + MONGO_USE_SSL) != null && properties.getProperty(prefix + DELIMITER + MONGO_USE_SSL).equalsIgnoreCase("true")) {
            indexingSettings.setMongoEnableSsl();
        }
    }

    private boolean mongoCredentialsProvidedFor(String prefix) {
        if (!"".equals(properties.get(prefix + DELIMITER + MONGO_USERNAME)) &&
                !"".equals(properties.get(prefix + DELIMITER + MONGO_PASSWORD)) &&
                !"".equals(properties.get(prefix + DELIMITER + MONGO_AUTH_DB))) {
            return true;
        } else {
            return false;
        }
    }
    private void prepareSolrSetting(IndexingSettings indexingSettings, String prefix) throws URISyntaxException, IndexerConfigurationException {
        String solrInstances = properties.get(prefix + DELIMITER + SOLR_INSTANCES).toString();
        String[] instances = solrInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addSolrHost(new URI(instance));
        }
    }

    private void prepareZookeeperSettings(IndexingSettings indexingSettings, String prefix) throws IndexerConfigurationException {
        String zookeeperInstances = properties.get(prefix + DELIMITER + ZOOKEEPER_INSTANCES).toString();
        int zookeeperPort = Integer.parseInt(properties.get(prefix + DELIMITER + ZOOKEEPER_PORT_NUMBER).toString());
        String[] instances = zookeeperInstances.split(",");
        for (String instance : instances) {
            indexingSettings.addZookeeperHost(new InetSocketAddress(instance, zookeeperPort));
        }
        indexingSettings.setZookeeperChroot(properties.getProperty(prefix + DELIMITER + ZOOKEEPER_CHROOT));
        indexingSettings.setZookeeperDefaultCollection(properties.getProperty(prefix + DELIMITER + ZOOKEEPER_DEFAULT_COLLECTION));
    }
}
