package eu.europeana.cloud.uidservice;

import eu.europeana.cloud.definitions.response.Response;

@SuppressWarnings("rawtypes")
public interface UniqueIdentifierService {

	public Response createRecordId(String providerId,String recordId);
	
	public Response getGlobalId(String providerId, String recordId);
	
	public Response getLocalIds(String globalId);
	
	public Response getLocalIdsByProvider(String providerId);
	
	public Response getGlobalIdsByProvider(String providerId);
	
	//public StringResponse getGlobalIdsByProviders(List<String> providerIds);
	
	public Response createMapping(String globalId,String providerId,String recordId);
	
	public Response createMappingsByOneProvider(String globalId,String providerId,String recordId);
	
	//public StringResponse createMappingsByManyProviders(String globalId, Map<String,List<String>> recordIdMap);
	
	public Response removeMapppingByLocalId(String providerId,String recordId);
	
	public Response deleteGlobalId(String globalId);
	
}
