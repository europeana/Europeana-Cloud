package eu.europeana.cloud.client.uis.rest.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
public class UrlProvider {


	private static String baseUrl;

	public UrlProvider(){
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(new File("src/main/resources/client.properties")));
			baseUrl = props.getProperty("server.baseUrl");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String createUrl(String url){
		return baseUrl+"/"+url;
	}
	
}
