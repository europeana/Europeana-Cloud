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

import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.CloudIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.CloudIdList;
import eu.europeana.cloud.service.uis.LocalIdList;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

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

	private static final String PROVIDERID="providerId";
	private static final String RECORDID="recordId";
	private static final String CLOUDID="cloudId";
	private static final String START = "start";
	private static final String TO="to";
	@GET
	@Path("createCloudIdLocal")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createCloudId(@QueryParam(PROVIDERID) String providerId,@QueryParam(RECORDID) String recordId) {
		try {
			return Response.ok().entity(uniqueIdentifierService.createCloudId(providerId, recordId)).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();

		} catch (RecordExistsException e) {
			return Response.status(IdentifierErrorInfo.RECORD_EXISTS.getHttpCode())
					.entity(IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId)).build();
		}
	}

	@GET
	@Path("createCloudIdNoLocal")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createCloudId(@QueryParam(PROVIDERID) String providerId) {
		
		try {
			return Response.ok().entity(uniqueIdentifierService.createCloudId(providerId)).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();

		} catch (RecordExistsException e) {
			return Response.status(IdentifierErrorInfo.RECORD_EXISTS.getHttpCode())
					.entity(IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId,"auto-generated")).build();
		}
	}
	
	@GET
	@Path("getCloudId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getCloudId(@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId) {
		try {
			return Response.ok(uniqueIdentifierService.getCloudId(providerId, recordId)).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (RecordDoesNotExistException e) {

			return Response.status(IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)).build();
		}
	}

	@GET
	@Path("getLocalIds")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIds(@QueryParam(CLOUDID) String cloudId) {
		try {
			LocalIdList pList = new LocalIdList();
			pList.setList(uniqueIdentifierService.getLocalIdsByCloudId(cloudId));
			return Response.ok(pList).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (CloudIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)).build();
		}
	}

	@GET
	@Path("getLocalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIdsByProvider(@QueryParam(PROVIDERID) String providerId,
			@QueryParam(START) String start, @QueryParam(TO) @DefaultValue("10000") int to) {
		try {
			LocalIdList pList = new LocalIdList();
			pList.setList(uniqueIdentifierService.getLocalIdsByProvider(providerId, start, to));
			return Response.ok(pList).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (ProviderDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)).build();
		} catch (RecordDatasetEmptyException e) {
			return Response.status(IdentifierErrorInfo.RECORDSET_EMPTY.getHttpCode())
					.entity(IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)).build();
		}

	}

	@GET
	@Path("getCloudIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getCloudIdsByProvider(@QueryParam(PROVIDERID) String providerId,
			@QueryParam(START) String start, @QueryParam(TO) @DefaultValue("10000") int to) {
		try {
			CloudIdList gList = new CloudIdList();
			gList.setList(uniqueIdentifierService.getCloudIdsByProvider(providerId, start, to));
			return Response.ok(gList).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (ProviderDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)).build();
		} catch(RecordDatasetEmptyException e){
			return Response.status(IdentifierErrorInfo.RECORDSET_EMPTY.getHttpCode())
					.entity(IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)).build();
	 	}
	}

	@GET
	@Path("createMapping")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createIdMapping(@QueryParam(CLOUDID) String cloudId,
			@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId) {
		try {
			uniqueIdentifierService.createIdMapping(cloudId, providerId, recordId);
			return Response.ok("Mapping created succesfully").build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (ProviderDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)).build();
		} catch (CloudIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)).build();
		} catch (IdHasBeenMappedException e) {
			return Response.status(IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getHttpCode())
					.entity(IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, cloudId))
					.build();
		} catch (RecordIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.RECORDID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.RECORDID_DOES_NOT_EXIST.getErrorInfo(recordId)).build();
		}
	}

	@DELETE
	@Path("removeMappingByLocalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response removeIdMapping(@QueryParam(PROVIDERID) String providerId, @QueryParam(RECORDID) String recordId) {
		try {
			uniqueIdentifierService.removeIdMapping(providerId, recordId);
			return Response.ok("Mapping marked as deleted").build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (ProviderDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)).build();
		} catch (RecordIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.RECORDID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.RECORDID_DOES_NOT_EXIST.getErrorInfo(recordId)).build();
		}
	}

	@DELETE
	@Path("deleteCloudId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response deleteCloudId(@QueryParam(CLOUDID) String cloudId) {
		try {
			uniqueIdentifierService.deleteCloudId(cloudId);
			return Response.ok("CloudId marked as deleted").build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (CloudIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)).build();
		}
	}
}
