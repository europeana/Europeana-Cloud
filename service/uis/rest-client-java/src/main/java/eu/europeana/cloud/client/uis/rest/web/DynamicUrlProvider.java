package eu.europeana.cloud.client.uis.rest.web;

import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.coordination.discovery.ZookeeperServiceDiscovery;
import eu.europeana.cloud.service.coordination.provider.ServiceProvider;
import eu.europeana.cloud.service.coordination.provider.ServiceProviderImpl;

/**
 * URL provider for UIS client
 * 
 * Returns a dynamically provided url.
 */
public class DynamicUrlProvider implements UrlProvider {

	private ServiceProvider provider;

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamicUrlProvider.class);
	
	private static final int CONNECTION_TIMEOUT = 3000;

	private static final int SESSION_TIMEOUT = 3000;

	private static final String UIS_SERVICE_KEY = "UIS";
	
	/**
	 * Creates a new instance of this class,
	 * since no URL is provided properties will be read from the properties file.
	 * 
	 * @throws IOException
	 */
	public DynamicUrlProvider() throws IOException {
		
		LOGGER.info("DynamicUrlProvider: Starting UrlProvider, no URL provided");
		
		String zkAddress = null;
		String preferredDatacenter = null;
		String serviceDiscoveryPath = null;
		
		Properties props = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(new File("src/main/resources/client.properties"));
			props.load(is);
			zkAddress = props.getProperty("coordination.service.URL");
			preferredDatacenter = props.getProperty("coordination.service.PREFERRED_DATACENTER");
			serviceDiscoveryPath = props.getProperty("coordination.service.SERVICE_DISCOVERY_PATH");
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (is != null) {
				is.close();
			}
		}

		ZookeeperService zS = new ZookeeperService(zkAddress, CONNECTION_TIMEOUT, SESSION_TIMEOUT, serviceDiscoveryPath);
		ZookeeperServiceDiscovery serviceDiscovery = new ZookeeperServiceDiscovery(zS, "/discovery", UIS_SERVICE_KEY);
		provider = new ServiceProviderImpl(serviceDiscovery, preferredDatacenter);

		LOGGER.info("DynamicUrlProvider: UrlProvider started successfully");
	}

	public DynamicUrlProvider(final String zkAddress, final String serviceDiscoveryPath, final String preferredDatacenter) {

		LOGGER.info("DynamicUrlProvider: Starting UrlProvider {}:{}:{}", zkAddress, serviceDiscoveryPath, preferredDatacenter);

		ZookeeperService zS = new ZookeeperService(zkAddress, CONNECTION_TIMEOUT, SESSION_TIMEOUT, serviceDiscoveryPath);
		ZookeeperServiceDiscovery serviceDiscovery = new ZookeeperServiceDiscovery(zS, serviceDiscoveryPath, UIS_SERVICE_KEY);
		provider = new ServiceProviderImpl(serviceDiscovery, preferredDatacenter);
		
		LOGGER.info("DynamicUrlProvider: UrlProvider started successfully.");
	}

	public DynamicUrlProvider(final ServiceProvider provider) {
		LOGGER.info("DynamicUrlProvider: Starting UrlProvider...");
		this.provider = provider;
		LOGGER.info("DynamicUrlProvider: UrlProvider started successfully.");
	}
	
	/**
	 * @return Returns a url 
	 */
	@Override
	public String getBaseUrl() {
		final String address = provider.getService().getListenAddress();
		LOGGER.info("DynamicUrlProvider: using url: {}", address);
		return address;
	}
}
