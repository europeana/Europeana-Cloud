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
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class UrlProvider {

	private String baseUrl;
	
	private ServiceProvider provider;

	private static final Logger LOGGER = LoggerFactory.getLogger(UrlProvider.class);
	
	private static final int CONNECTION_TIMEOUT = 10000;

	private static final int SESSION_TIMEOUT = 10000;

	private static final String UIS_SERVICE_KEY = "UIS";
	
	/**
	 * Creates a new instance of this class,
	 * since no URL is provided Zookeeper will be used for service discovery.
	 * 
	 * @throws IOException
	 */
	public UrlProvider() throws IOException {
		
		LOGGER.info("Starting UrlProvider, no URL provided");
		
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
		ZookeeperServiceDiscovery serviceDiscovery = new ZookeeperServiceDiscovery(zS, "",UIS_SERVICE_KEY);
		provider = new ServiceProviderImpl(serviceDiscovery, preferredDatacenter);
		
		baseUrl = provider.getService().getListenAddress();

		LOGGER.info("Starting UrlProvider with serviceUrl='{}'", baseUrl);
	}

	/**
	 * Creates a new instance of this class.
	 * 
	 * @param serviceUrl
	 */
	public UrlProvider(final String serviceUrl) {
		LOGGER.info("Starting UrlProvider with serviceUrl='{}'", serviceUrl);
		baseUrl = serviceUrl;
		LOGGER.info("UrlProvider started successfully");
	}
	

	/**
	 * Return the base url for the unique identifier service
	 * 
	 * @return The base url for the service
	 */
	public String getBaseUrl() {
		return baseUrl;
	}
}
