package eu.europeana.cloud.uidservice.rest.impl;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.definitions.StatusCode;
import eu.europeana.cloud.definitions.response.ListResponse;
import eu.europeana.cloud.definitions.response.Response;
import eu.europeana.cloud.definitions.response.StringResponse;
import eu.europeana.cloud.definitions.response.VoidResponse;
import eu.europeana.cloud.uidservice.rest.UniqueIdResource;
import eu.europeana.cloud.uidservice.service.UniqueIdService;

@Component
@SuppressWarnings("rawtypes")
@Path("uniqueid")
public class UniqueIdResourceMock implements UniqueIdResource {

	@Autowired
	private UniqueIdService uniqueIdService;

	@GET
	@Path("createRecordId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createRecordId(@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {

		Response<String> response = new StringResponse();
		String globalId = null;
		System.out.println(uniqueIdService == null);
		try {
			globalId = uniqueIdService.create(providerId, recordId);
			response.setStatusCode(StatusCode.OK);
			response.setResponse(globalId);
		} catch (Exception e) {
			e.printStackTrace();
			response.setStatusCode(StatusCode.DATABASECONNECTIONERROR);
			response.setResponse(StatusCode.DATABASECONNECTIONERROR.getDescription(providerId, recordId, "", ""));
		}
		return response;
	}

	@GET
	@Path("getGlobalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getGlobalId(@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {
		Response<String> str = new StringResponse();
		return str;
	}

	@GET
	@Path("getLocalIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIds(@QueryParam("globalId") String globalId) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

	@GET
	@Path("getLocalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIdsByProvider(@QueryParam("providerId") String providerId, @QueryParam("start") int start,
			@QueryParam("to") int to) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

	@GET
	@Path("getGlobalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getGlobalIdsByProvider(@QueryParam("providerId") String providerId, @QueryParam("start") int start,
			@QueryParam("to") int to) {
		Response<List<String>> response = new ListResponse();
		return response;
	}

	@GET
	@Path("createMapping")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createMapping(@QueryParam("globalId") @DefaultValue("test") String globalId,
			@QueryParam("providerId") @DefaultValue("test") String providerId,
			@QueryParam("recordId") @DefaultValue("test") String recordId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

	@DELETE
	@Path("removeMappingByLocalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response removeMappingByLocalId(@QueryParam("providerId") @DefaultValue("test") String providerId,
			@QueryParam("recordId") @DefaultValue("test") String recordId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

	@DELETE
	@Path("deleteGlobalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response deleteGlobalId(@QueryParam("globalId") @DefaultValue("test") String globalId) {
		Response<Void> response = new VoidResponse();
		return response;
	}

}
