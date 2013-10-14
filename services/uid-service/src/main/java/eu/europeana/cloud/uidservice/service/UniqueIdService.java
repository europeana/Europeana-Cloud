package eu.europeana.cloud.uidservice.service;

public interface UniqueIdService {

	String create(String providerId, String recordId);

}