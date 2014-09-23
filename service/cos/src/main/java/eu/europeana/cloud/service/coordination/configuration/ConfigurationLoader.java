package eu.europeana.cloud.service.coordination.configuration;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.AbstractFactoryBean;

import eu.europeana.cloud.service.coordination.ZookeeperService;

/**
 * Loads properties from a Zookeeper node (specified in {@link #configurationSettingsPath}).
 */
public class ConfigurationLoader extends AbstractFactoryBean<Properties> implements Watcher {

	private Logger LOGGER = LoggerFactory.getLogger(ConfigurationLoader.class);
	
	private ZookeeperService zookeeperService;
	
	private String configurationSettingsPath;

	@Override
	public Properties createInstance() throws Exception {
		Properties p = new Properties();
		p.load(new ByteArrayInputStream(loadFromZk()));
		return p;
	}

	@Override
	public Class<Properties> getObjectType() {
		return Properties.class;
	}

	private byte[] loadFromZk() throws Exception {
		return zookeeperService.getClient().getData().forPath(configurationSettingsPath);
	}

	@Override
	public void process(WatchedEvent event) {
	}

	public void setZookeeperService(ZookeeperService zookeeperService) {
		this.zookeeperService = zookeeperService;
	}

	public void setConfigurationSettingsPath(String configurationSettingsPath) {
		this.configurationSettingsPath = configurationSettingsPath;
	}
}