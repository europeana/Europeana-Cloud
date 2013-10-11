package eu.europeana.cloud.uidservice.tools;

public class UniqueIdCreator {

	public static String create(String providerId,String recordId){
		return String.format("/%s/%s", providerId,recordId);
	}
}
