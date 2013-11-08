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
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
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

	@GET
	@Path("createRecordId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response createGlobalId(@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {
		try {
			return Response.ok().entity(uniqueIdentifierService.createGlobalId(providerId, recordId)).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();

		} catch (RecordExistsException e) {
			return Response.status(IdentifierErrorInfo.RECORD_EXISTS.getHttpCode())
					.entity(IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId)).build();
		}
	}

	@GET
	@Path("getGlobalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getGlobalId(@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {
		try {
			return Response.ok(uniqueIdentifierService.getGlobalId(providerId, recordId)).build();
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
	public Response getLocalIds(@QueryParam("globalId") String globalId) {
		try {
			LocalIdList pList = new LocalIdList();
			pList.setList(uniqueIdentifierService.getLocalIdsByGlobalId(globalId));
			return Response.ok(pList).build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (GlobalIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo(globalId)).build();
		}
	}

	@GET
	@Path("getLocalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getLocalIdsByProvider(@QueryParam("providerId") String providerId,
			@QueryParam("start") @DefaultValue("0") int start, @QueryParam("to") @DefaultValue("10000") int to) {
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
	@Path("getGlobalIdsByProvider")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response getGlobalIdsByProvider(@QueryParam("providerId") String providerId,
			@QueryParam("start") @DefaultValue("0") int start, @QueryParam("to") @DefaultValue("10000") int to) {
		try {
			CloudIdList gList = new CloudIdList();
			gList.setList(uniqueIdentifierService.getGlobalIdsByProvider(providerId, start, to));
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
	public Response createIdMapping(@QueryParam("globalId") String globalId,
			@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {
		try {
			uniqueIdentifierService.createIdMapping(globalId, providerId, recordId);
			return Response.ok("Mapping created succesfully").build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (ProviderDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)).build();
		} catch (GlobalIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo(globalId)).build();
		} catch (IdHasBeenMappedException e) {
			return Response.status(IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getHttpCode())
					.entity(IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, globalId))
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
	public Response removeIdMapping(@QueryParam("providerId") String providerId, @QueryParam("recordId") String recordId) {
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
	@Path("deleteGlobalId")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
	@Override
	public Response deleteGlobalId(@QueryParam("globalId") String globalId) {
		try {
			uniqueIdentifierService.deleteGlobalId(globalId);
			return Response.ok("GlobalId marked as deleted").build();
		} catch (DatabaseConnectionException e) {
			return Response.status(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getHttpCode())
					.entity(IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo("", "", e.getMessage())).build();
		} catch (GlobalIdDoesNotExistException e) {
			return Response.status(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getHttpCode())
					.entity(IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo(globalId)).build();
		}
	}
}
