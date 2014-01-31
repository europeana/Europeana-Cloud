package eu.europeana.cloud.client.uis.rest.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

	private static final Logger logger = LoggerFactory.getLogger(UrlProvider.class);
	/**
	 * Creates a new instance of this class.
	 */
	public UrlProvider(){
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(new File("src/main/resources/client.properties")));
			baseUrl = props.getProperty("server.baseUrl");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

        /**
         * Creates a new instance of this class.
         * @param serviceUrl
         */
        public UrlProvider(final String serviceUrl) {
            baseUrl = serviceUrl;
        }

	/**
	 * Return the host url for the unique identifier service
	 * @param url
	 * @return The host url for the service
	 */
	public String getUidUrl(String url){
		return baseUrl+"/uniqueId/"+url;
	}
	
	/**
	 * Return the host url for the data providers service
	 * @param url
	 * @return The host url for the data providers service
	 */
	public String getPidUrl(String url){
		return baseUrl+"/uniqueId/data-providers"+url;
	}
}
