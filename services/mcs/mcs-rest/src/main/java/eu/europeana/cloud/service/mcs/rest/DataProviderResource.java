package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * Resource for DataProviders
 * 
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
@Component
public class DataProviderResource {

    @Autowired
    private DataProviderService providerService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @Context
    private UriInfo uriInfo;


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public DataProvider getProvider()
            throws ProviderNotExistsException {
        return providerService.getProvider(providerId);
    }


    @PUT
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Response updateProvider(DataProviderProperties dataProviderProperties) {
        DataProvider provider = providerService.updateProvider(providerId, dataProviderProperties);
        EnrichUriUtil.enrich(uriInfo, provider);
        return Response.noContent().build();
    }


    @DELETE
    public void deleteProvider()
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        providerService.deleteProvider(providerId);
    }
}
