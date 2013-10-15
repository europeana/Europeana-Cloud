package eu.europeana.cloud.uidservice.rest;

import javax.ws.rs.core.Response;

public interface UniqueIdResource {

	public Response createRecordId(String providerId,String recordId);
	
	public Response getGlobalId(String providerId, String recordId);
	
	public Response getLocalIds(String globalId);
	
	public Response getLocalIdsByProvider(String providerId, int start, int to);
	
	public Response getGlobalIdsByProvider(String providerId, int start, int to);
	
	public Response createMapping(String globalId,String providerId,String recordId);
	
	public Response removeMappingByLocalId(String providerId,String recordId);
	
	public Response deleteGlobalId(String globalId);
	
}
