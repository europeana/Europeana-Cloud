package eu.europeana.cloud.uidservice.impl;

import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import eu.europeana.cloud.definitions.StatusCode;
import eu.europeana.cloud.definitions.response.ListResponse;
import eu.europeana.cloud.definitions.response.Response;
import eu.europeana.cloud.definitions.response.StringResponse;
import eu.europeana.cloud.definitions.response.VoidResponse;
import eu.europeana.cloud.uidservice.UniqueIdentifierService;
import eu.europeana.cloud.uidservice.tools.UniqueIdCreator;

@Path("uniqueid")
public class UniqueIdentifierServiceMock implements UniqueIdentifierService {

	@GET
	@Path("createRecordId")
	@Produces(MediaType.APPLICATION_JSON)
	@Override
	public Response createRecordId(@QueryParam("providerId") @DefaultValue("test") String providerId,
			@QueryParam("recordId") @DefaultValue("test") String recordId) {

		Response<String> response = new StringResponse();
		String globalId = null;
		try {
			globalId = UniqueIdCreator.create(providerId, recordId);
			response.setStatusCode(StatusCode.OK.getCode());
			response.setResponse(globalId);
		} catch (Exception e) {
			response.setStatusCode(StatusCode.DATABASECONNECTIONERROR.getCode());
			response.setResponse(StatusCode.DATABASECONNECTIONERROR.getDescription(providerId,recordId));
		}
		return response;
	}

	@Override
	public Response getGlobalId(String providerId, String recordId) {
		Response<String> str = new StringResponse();
		return str;
	}

	@Override
	public Response getLocalIds(String globalId) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

	@Override
	public Response getLocalIdsByProvider(String providerId) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

	@Override
	public Response getGlobalIdsByProvider(String providerId) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

//	@Override
//	public StringResponse getGlobalIdsByProviders(List<String> providerIds) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public Response createMapping(String globalId, String providerId, String recordId) {
		Response<Void> response= new VoidResponse();
		return response;
	}

	@Override
	public Response createMappingsByOneProvider(String globalId, String providerId, String recordId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

//	@Override
//	public StringResponse createMappingsByManyProviders(String globalId, Map<String, List<String>> recordIdMap) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	@Override
	public Response removeMapppingByLocalId(String providerId, String recordId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

	@Override
	public Response deleteGlobalId(String globalId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

}
