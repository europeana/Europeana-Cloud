package eu.europeana.cloud.client.uis.rest.web;

public class UrlProvider {

	
	private static String baseUrl;
	
	public static String createUrl(String url){
		return baseUrl+"/"+url;
	}
	
}
