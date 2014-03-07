package eu.europeana.cloud.client.uis.rest.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL provider for UIS client
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class UrlProvider {

	private static String baseUrl;

	private static final Logger LOGGER = LoggerFactory.getLogger(UrlProvider.class);

	/**
	 * Creates a new instance of this class.
	 * 
	 * @throws IOException
	 */
	public UrlProvider() throws IOException {
		Properties props = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(new File("src/main/resources/client.properties"));
			props.load(is);
			baseUrl = props.getProperty("server.baseUrl");
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		} finally {
			if (is != null) {
				is.close();
			}
		}
	}

	/**
	 * Creates a new instance of this class.
	 * 
	 * @param serviceUrl
	 */
	public UrlProvider(final String serviceUrl) {
		baseUrl = serviceUrl;
	}
	

	/**
	 * Return the base url for the unique identifier service
	 * 
	 * @param url
	 * @return The base url for the service
	 */
	public String getBaseUrl() {
		return baseUrl;
	}
}
