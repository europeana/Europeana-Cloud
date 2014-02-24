package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * Implementation of the Unique Identifier Service. Accessible path /uniqueid
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@Component
@Path("/")
@Scope("request")
public class BasicUniqueIdResource{
	@Autowired
	private UniqueIdentifierService uniqueIdentifierService;

	private static final String PROVIDERID = "providerId";
	private static final String LOCALID = "localId";
	private static final String CLOUDID = "cloudId";
	
	@PathParam(CLOUDID)
	private String cloudId;
	
	@POST
	@Path("cloudIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@ReturnType("eu.europeana.cloud.common.model.CloudId")
	public Response createCloudId(@QueryParam(PROVIDERID) String providerId, @QueryParam(LOCALID) String localId)
			throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
			RecordDatasetEmptyException, CloudIdDoesNotExistException {

		return localId != null ? Response.ok().entity(uniqueIdentifierService.createCloudId(providerId, localId))
				.build() : Response.ok().entity(uniqueIdentifierService.createCloudId(providerId)).build();
	}

	@GET
	@Path("cloudIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@ReturnType("eu.europeana.cloud.common.model.CloudId")
	public Response getCloudId(@QueryParam(PROVIDERID) String providerId, @QueryParam(LOCALID) String recordId)
			throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {
		return Response.ok(uniqueIdentifierService.getCloudId(providerId, recordId)).build();
	}

	@GET
	@Path("cloudIds/{"+CLOUDID+"}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@ReturnType("eu.europeana.cloud.common.response.ResultSlice")
	public Response getLocalIds() throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		ResultSlice<CloudId> pList = new ResultSlice<>();
		pList.setResults(uniqueIdentifierService.getLocalIdsByCloudId(cloudId));
		return Response.ok(pList).build();
	}

	@DELETE
	@Path("cloudIds/{"+CLOUDID+"}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	public Response deleteCloudId() throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
		uniqueIdentifierService.deleteCloudId(cloudId);
		return Response.ok("CloudId marked as deleted").build();
	}
}
