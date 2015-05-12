package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.uis.DataProviderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

@Path("/data-providers/{" + P_PROVIDER + "}/active")
@Component
@Scope("request")
public class DataProviderActivationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProviderActivationResource.class);

    @Autowired
    private DataProviderService providerService;

    /**
     * Activates data provider (sets flag 'active' to true)
     * 
     * <br/><br/>
     * <strong>Required permissions:</strong>
     * <ul>
     *     <li>Admin role</li>
     * </ul>
     * 
     * @summary Data-provider activation 
     * @param dataProviderId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated 
     * @return Empty response with http status code indicating whether the operation was successful or not
     * @author 
     * @throws ProviderDoesNotExistException Supplied Data-provider does not exist
     * 
     */
    @PUT
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response activateDataProvider(@PathParam(P_PROVIDER) String dataProviderId) throws ProviderDoesNotExistException {
        LOGGER.info("Activating data provider: {}", dataProviderId);

        DataProvider dataProvider = providerService.getProvider(dataProviderId);
        dataProvider.setActive(true);
        providerService.updateProvider(dataProvider);
        
        return Response.ok().build();
    }

    /**
     * Deactivates data provider (sets flag 'active' to false)
     * 
     * <br/><br/>
     * <strong>Required permissions:</strong>
     * <ul>
     *     <li>Admin role</li>
     * </ul>
     * 
     * @summary Data-provider deactivation
     * @param dataProviderId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated    
     * @return Empty response with http status code indicating whether the operation was successful or not
     * @author
     * @throws ProviderDoesNotExistException Supplied Data-provider does not exist 
     */
    @DELETE
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response deactivateDataProvider(@PathParam(P_PROVIDER) String dataProviderId) throws ProviderDoesNotExistException {
        LOGGER.info("Deactivating data provider: {}", dataProviderId);

        DataProvider dataProvider = providerService.getProvider(dataProviderId);
        dataProvider.setActive(false);
        providerService.updateProvider(dataProvider);
        
        return Response.ok().build();
    }

}
