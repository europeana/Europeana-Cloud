package eu.europeana.cloud.service.uis.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.JerseyClientBuilder;

/**
 * Proxy class to connect to the Metadata and Content Service
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class MCSProxy {

	private String url;
	
	/**
	 * The base url where MCS REST API is
	 */
	public MCSProxy(){
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(new File("src/main/resources/client.properties")));
			url = props.getProperty("mcs.baseUrl");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method that checks whether a provider exists or not
	 * @param providerId The providerId to search on
	 * @return true if it exists false otherwise
	 */
	public boolean checkProvider(String providerId){
		Client client = JerseyClientBuilder.newClient();
		Response rs = client.target(url+String.format("/data-providers/{%s}",providerId)).request().get();
		return rs.getStatusInfo()==Response.Status.OK;
	}
	
}
