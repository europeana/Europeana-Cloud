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

import eu.europeana.cloud.common.response.GenericCloudResponseGenerator;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.status.IdentifierCloudStati;

/**
 * Implementation of the Unique Identifier Service. Accessible path /uniqueid
 * 
 * @see eu.europeana.cloud.uidservice.rest.UniquedIdResource
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
    public Response createGlobalId(@QueryParam("providerId") String providerId,
            @QueryParam("recordId") String recordId) {
        try {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(),
                    uniqueIdentifierService.createGlobalId(providerId, recordId));
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (RecordExistsException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.RECORD_EXISTS.getCloudStatus(providerId, recordId), null);
        }
    }

    @GET
    @Path("getGlobalId")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response getGlobalId(@QueryParam("providerId") String providerId,
            @QueryParam("recordId") String recordId) {
        try {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(),
                    uniqueIdentifierService.getGlobalId(providerId, recordId));
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (RecordDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.RECORD_DOES_NOT_EXIST.getCloudStatus(providerId, recordId),
                    null);
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
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), pList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.GLOBALID_DOES_NOT_EXIST.getCloudStatus(globalId), null);
        }
    }

    @GET
    @Path("getLocalIdsByProvider")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response getLocalIdsByProvider(@QueryParam("providerId") String providerId,
            @QueryParam("start") @DefaultValue("0") int start,
            @QueryParam("to") @DefaultValue("10000") int to) {
        try {
            LocalIdList pList = new LocalIdList();
            pList.setList(uniqueIdentifierService.getLocalIdsByProvider(providerId, start, to));
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), pList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.PROVIDER_DOES_NOT_EXIST.getCloudStatus(providerId), null);
        } catch (RecordDatasetEmptyException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.RECORDSET_EMPTY.getCloudStatus(providerId), null);
        }

    }

    @GET
    @Path("getGlobalIdsByProvider")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response getGlobalIdsByProvider(@QueryParam("providerId") String providerId,
            @QueryParam("start") @DefaultValue("0") int start,
            @QueryParam("to") @DefaultValue("10000") int to) {
        try {
            GlobalIdList gList = new GlobalIdList();
            gList.setList(uniqueIdentifierService.getGlobalIdsByProvider(providerId, start, to));
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), gList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.PROVIDER_DOES_NOT_EXIST.getCloudStatus(providerId), null);
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
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), "Mapping created succesfully");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.PROVIDER_DOES_NOT_EXIST.getCloudStatus(providerId), null);
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.GLOBALID_DOES_NOT_EXIST.getCloudStatus(globalId), null);
        } catch (IdHasBeenMappedException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.ID_HAS_BEEN_MAPPED.getCloudStatus(recordId, providerId,
                            globalId), null);
        } catch (RecordIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.RECORDID_DOES_NOT_EXIST.getCloudStatus(recordId), null);
        }
    }

    @DELETE
    @Path("removeMappingByLocalId")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response removeIdMapping(@QueryParam("providerId") String providerId,
            @QueryParam("recordId") String recordId) {
        try {
            uniqueIdentifierService.removeIdMapping(providerId, recordId);
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), "Mapping marked as deleted");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.PROVIDER_DOES_NOT_EXIST.getCloudStatus(providerId), null);
        } catch (RecordIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.RECORDID_DOES_NOT_EXIST.getCloudStatus(recordId), null);
        }
    }

    @DELETE
    @Path("deleteGlobalId")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response deleteGlobalId(@QueryParam("globalId") String globalId) {
        try {
            uniqueIdentifierService.deleteGlobalId(globalId);
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.OK.getCloudStatus(), "GlobalId marked as deleted");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.DATABASE_CONNECTION_ERROR.getCloudStatus("", "", "",
                            e.getMessage()), null);
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    IdentifierCloudStati.GLOBALID_DOES_NOT_EXIST.getCloudStatus(globalId), null);
        }
    }
}
