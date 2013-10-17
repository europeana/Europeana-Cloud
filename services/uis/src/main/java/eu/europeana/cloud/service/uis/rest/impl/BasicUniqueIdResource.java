package eu.europeana.cloud.service.uis.rest.impl;

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

import eu.europeana.cloud.common.StatusCode;
import eu.europeana.cloud.common.response.GenericCloudResponseGenerator;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.rest.UniqueIdResource;
import eu.europeana.cloud.service.uis.service.UniqueIdentifierService;

/**
 * Implementation of the Unique Identifier Service. Accessible path /uniqueid
 * 
 * @see eu.europeana.cloud.uidservice.rest.UniquedIdResource
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@Component
@Path("uniqueid")
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK,
                    uniqueIdentifierService.createGlobalId(providerId, recordId));
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (RecordExistsException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.RECORDEXISTS,
                    StatusCode.RECORDEXISTS.getDescription(providerId, recordId, ""));
        }
    }

    @GET
    @Path("getGlobalId")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response getGlobalId(@QueryParam("providerId") String providerId,
            @QueryParam("recordId") String recordId) {
        try {
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK,
                    uniqueIdentifierService.getGlobalId(providerId, recordId));
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (RecordDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.RECORDDOESNOTEXIST,
                    StatusCode.RECORDDOESNOTEXIST.getDescription(providerId, recordId));
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK, pList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.GLOBALIDDOESNOTEXIST,
                    StatusCode.GLOBALIDDOESNOTEXIST.getDescription(globalId));
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK, pList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.PROVIDERDOESNOTEXIST,
                    StatusCode.PROVIDERDOESNOTEXIST.getDescription(providerId));
        } catch (RecordDatasetEmptyException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.RECORDSETEMPTY,
                    StatusCode.RECORDSETEMPTY.getDescription(providerId));
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK, gList);
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.PROVIDERDOESNOTEXIST,
                    StatusCode.PROVIDERDOESNOTEXIST.getDescription(providerId));
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK,
                    "Mapping created succesfully");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.PROVIDERDOESNOTEXIST,
                    StatusCode.PROVIDERDOESNOTEXIST.getDescription(providerId));
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.GLOBALIDDOESNOTEXIST,
                    StatusCode.GLOBALIDDOESNOTEXIST.getDescription(globalId));
        } catch (IdHasBeenMappedException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.IDHASBEENMAPPED,
                    StatusCode.IDHASBEENMAPPED.getDescription(recordId, providerId, globalId));
        } catch (RecordIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.RECORDIDDOESNOTEXIST,
                    StatusCode.RECORDIDDOESNOTEXIST.getDescription(recordId));
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
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK,
                    "Mapping marked as deleted");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (ProviderDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.PROVIDERDOESNOTEXIST,
                    StatusCode.PROVIDERDOESNOTEXIST.getDescription(providerId));
        } catch (RecordIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.RECORDIDDOESNOTEXIST,
                    StatusCode.RECORDIDDOESNOTEXIST.getDescription(recordId));
        }
    }

    @DELETE
    @Path("deleteGlobalId")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Override
    public Response deleteGlobalId(@QueryParam("globalId") String globalId) {
        try {
            uniqueIdentifierService.deleteGlobalId(globalId);
            return GenericCloudResponseGenerator.generateCloudResponse(StatusCode.OK,
                    "GlobalId marked as deleted");
        } catch (DatabaseConnectionException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.DATABASECONNECTIONERROR,
                    StatusCode.DATABASECONNECTIONERROR.getDescription("", "", "", e.getMessage()));
        } catch (GlobalIdDoesNotExistException e) {
            return GenericCloudResponseGenerator.generateCloudResponse(
                    StatusCode.GLOBALIDDOESNOTEXIST,
                    StatusCode.GLOBALIDDOESNOTEXIST.getDescription(globalId));
        }
    }
}
