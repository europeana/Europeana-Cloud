package eu.europeana.cloud.service.uis.rest;

import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_LOCALID;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * Resource for DataProvider.
 * 
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
@Component
@Scope("request")
public class DataProviderResource {

    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

    @Autowired
    private DataProviderService providerService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @PathParam(P_LOCALID)
    private String localId;

    @PathParam(P_CLOUDID)
    private String cloudId;

    @Context
    private UriInfo uriInfo;

    private static final String FROM = "from";
    private static final String TO = "to";


    /**
     * Gets provider.
     * 
     * @return Data provider.
     * @throws ProviderDoesNotExistException
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public DataProvider getProvider()
            throws ProviderDoesNotExistException {
        return providerService.getProvider(providerId);
    }


    /**
     * Updates data provider information. *
     * 
     * @param dataProviderProperties
     *            data provider properties.
     * @throws ProviderDoesNotExistException
     * @statuscode 204 object has been updated.
     */
    @PUT
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public void updateProvider(DataProviderProperties dataProviderProperties)
            throws ProviderDoesNotExistException {
        DataProvider provider = providerService.updateProvider(providerId, dataProviderProperties);
        EnrichUriUtil.enrich(uriInfo, provider);
    }


    /**
     * @param start
     * @param to
     * @return
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    @GET
    @Path("localIds")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice")
    public Response getLocalIdsByProvider(@QueryParam(FROM) String from, @QueryParam(TO) @DefaultValue("10000") int to)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        ResultSlice<CloudId> pList = new ResultSlice<>();
        pList.setResults(uniqueIdentifierService.getLocalIdsByProvider(providerId, from, to));
        if (pList.getResults().size() == to) {
            pList.setNextSlice(pList.getResults().get(to - 1).getId());
        }
        return Response.ok(pList).build();

    }


    @GET
    @Path("cloudIds")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice")
    public Response getCloudIdsByProvider(@QueryParam(FROM) String from, @QueryParam(TO) @DefaultValue("10000") int to)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        ResultSlice<CloudId> pList = new ResultSlice<>();
        pList.setResults(uniqueIdentifierService.getCloudIdsByProvider(providerId, from, to));
        if (pList.getResults().size() == to) {
            pList.setNextSlice(pList.getResults().get(to - 1).getId());
        }
        return Response.ok(pList).build();
    }


    @PUT
    @Path("cloudIds/{" + P_CLOUDID + "}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public Response createIdMapping(@QueryParam("localId") String localId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, RecordDatasetEmptyException {
        if (localId != null) {
            return Response.ok().entity(uniqueIdentifierService.createIdMapping(cloudId, providerId, localId)).build();
        } else {
            return Response.ok().entity(uniqueIdentifierService.createIdMapping(cloudId, providerId)).build();
        }
    }


    @DELETE
    @Path("localIds/{" + P_LOCALID + "}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public Response removeIdMapping()
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
        uniqueIdentifierService.removeIdMapping(providerId, localId);
        return Response.ok("Mapping marked as deleted").build();
    }

}
