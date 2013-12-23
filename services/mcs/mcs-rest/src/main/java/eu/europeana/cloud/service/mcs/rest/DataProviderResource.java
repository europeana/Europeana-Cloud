package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

/**
 * Resource for DataProvider.
 * 
 */
@Path("/data-providers/{" + P_PROVIDER + "}")
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
     * @throws ProviderNotExistsException
     *             resource that represents requested data provider does not exist.
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public DataProvider getProvider()
            throws ProviderNotExistsException {
        return providerService.getProvider(providerId);
    }


    /**
     * Updates data provider information. *
     * 
     * @param dataProviderProperties
     *            data provider properties.
     * @throws ProviderNotExistsException
     *             no such provider exists.
     * @statuscode 204 object has been updated.
     */
    @PUT
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public void updateProvider(DataProviderProperties dataProviderProperties)
            throws ProviderNotExistsException {
        DataProvider provider = providerService.updateProvider(providerId, dataProviderProperties);
        EnrichUriUtil.enrich(uriInfo, provider);
    }


    /**
     * Deletes data provider.
     * 
     * @throws ProviderNotExistsException
     *             no such provider exists
     * @throws ProviderHasDataSetsException
     *             data provider cannot be deleted because he has data sets. All objects created by data provider must
     *             be deleted before data provider can be deleted.
     * @throws ProviderHasRecordsException
     *             data provider cannot be deleted because he created some representation versions. All objects created
     *             by data provider must be deleted before data provider can be deleted.
     */
    @DELETE
    public void deleteProvider()
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        providerService.deleteProvider(providerId);
    }
}
