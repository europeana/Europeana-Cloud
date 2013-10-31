package eu.europeana.cloud.service.mcs.rest;

import java.util.List;
import javax.ws.rs.Consumes;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.DataProviderService;

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

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.DataSetService;

/**
 * Resource for DataProviders
 *
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
@Component
public class DataProviderResource {

    @Autowired
    DataProviderService providerService;

    @Autowired
    private DataSetService dataSetService;

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


    @PUT
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response createProvider(DataProviderProperties dataProviderProperties) {
        DataProvider provider = providerService.createProvider(providerId, dataProviderProperties);
        EnrichUriUtil.enrich(uriInfo, provider);
        return Response.created(provider.getUri()).build();
    }


    @DELETE
    public void deleteProvider()
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        List<DataSet> providerDataSets = dataSetService.getDataSets(providerId);
        if (providerDataSets != null && !providerDataSets.isEmpty()) {
            throw new ProviderHasDataSetsException();
        }
        providerService.deleteProvider(providerId);
    }
}
