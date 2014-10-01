package eu.europeana.cloud.client.uis.rest.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL provider for UIS client
 * 
 * Always returns the same URL.
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class StaticUrlProvider implements UrlProvider {

	private String baseUrl;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StaticUrlProvider.class);
	
	/**
	 * Creates a new instance of this class.
	 * 
	 * @param serviceUrl
	 */
	public StaticUrlProvider(final String serviceUrl) {
		LOGGER.info("StaticUrlProvider: starting UrlProvider with serviceUrl='{}'", serviceUrl);
		baseUrl = serviceUrl;
		LOGGER.info("StaticUrlProvider: urlProvider started successfully.");
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
