package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.service.DataProviderService;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Resource for DataProviders
 *
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
@Component
public class DataProviderResource {

    @Autowired
    DataProviderService providerService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @Context
    private UriInfo uriInfo;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public DataProvider getProvider()
            throws ProviderNotExistsException {
        return providerService.getProvider(providerId);
    }

    //add parameter for provider properties

    @PUT
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response createProvider() {
        DataProvider provider = providerService.createProvider(providerId);
        EnrichUriUtil.enrich(uriInfo, provider);
        return Response.created(provider.getUri()).build();
    }


    @DELETE
    public void deleteProvider()
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        providerService.deleteProvider(providerId);
    }
}
