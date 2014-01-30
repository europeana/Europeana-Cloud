package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.DataProviderService;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;
/**
 * Resource for DataProvider.
 * 
 */
@Path("/uniqueId/data-providers/{" + P_PROVIDER + "}")
@Component
@Scope("request")
public class DataProviderResource {

    @Autowired
    private DataProviderService providerService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @Context
    private UriInfo uriInfo;


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


}
