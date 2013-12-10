package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.service.uis.CloudIdList;
import eu.europeana.cloud.service.uis.LocalIdList;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * Implementation of the Unique Identifier Service. Accessible path /uniqueid
 * 
 * @see UniqueIdResource
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@Component
@Path("uniqueId")
public class BasicUniqueIdResource implements UniqueIdResource {
	@Autowired
	private UniqueIdentifierService uniqueIdentifierService;

	private static final String PROVIDERID = "providerId";
	private static final String RECORDID = "recordId";
	private static final String CLOUDID = "cloudId";
	private static final String START = "start";
	private static final String TO = "to";

	@GET
	@Path("createCloudIdLocal")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	@ReturnType("eu.europeana.cloud.common.model.CloudId")
	public Response createCloudId(@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId)
			throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdDoesNotExistException {
		return Response.ok().entity(uniqueIdentifierService.createCloudId(providerId, recordId)).build();
	}

	@GET
	@Path("createCloudIdNoLocal")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createCloudId(@QueryParam(PROVIDERID) String providerId) throws DatabaseConnectionException,
			RecordExistsException, ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdDoesNotExistException {

		return Response.ok().entity(uniqueIdentifierService.createCloudId(providerId)).build();
	}

	@GET
	@Path("getCloudId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getCloudId(@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId)
			throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		return Response.ok(uniqueIdentifierService.getCloudId(providerId, recordId)).build();
	}

	@GET
	@Path("getLocalIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIds(@QueryParam(CLOUDID) String cloudId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		LocalIdList pList = new LocalIdList();
		pList.setList(uniqueIdentifierService.getLocalIdsByCloudId(cloudId));
		return Response.ok(pList).build();
	}

	@GET
	@Path("getLocalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIdsByProvider(@QueryParam(PROVIDERID) String providerId, @QueryParam(START) String start,
			@QueryParam(TO) @DefaultValue("10000") int to) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		LocalIdList pList = new LocalIdList();
		pList.setList(uniqueIdentifierService.getLocalIdsByProvider(providerId, start, to));
		return Response.ok(pList).build();

	}

	@GET
	@Path("getCloudIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getCloudIdsByProvider(@QueryParam(PROVIDERID) String providerId, @QueryParam(START) String start,
			@QueryParam(TO) @DefaultValue("10000") int to) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		CloudIdList gList = new CloudIdList();
		gList.setList(uniqueIdentifierService.getCloudIdsByProvider(providerId, start, to));
		return Response.ok(gList).build();
	}

	@GET
	@Path("createMapping")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createIdMapping(@QueryParam(CLOUDID) String cloudId, @QueryParam(PROVIDERID) String providerId,
			@QueryParam(RECORDID) String recordId) throws DatabaseConnectionException, CloudIdDoesNotExistException,
			IdHasBeenMappedException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		uniqueIdentifierService.createIdMapping(cloudId, providerId, recordId);
		return Response.ok("Mapping created succesfully").build();
	}

	@DELETE
	@Path("removeMappingByLocalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response removeIdMapping(@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
		uniqueIdentifierService.removeIdMapping(providerId, recordId);
		return Response.ok("Mapping marked as deleted").build();
	}

	@DELETE
	@Path("deleteCloudId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response deleteCloudId(@QueryParam(CLOUDID) String cloudId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
		uniqueIdentifierService.deleteCloudId(cloudId);
		return Response.ok("CloudId marked as deleted").build();
	}
}
