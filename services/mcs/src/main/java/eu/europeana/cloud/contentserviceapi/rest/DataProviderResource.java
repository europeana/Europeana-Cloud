package eu.europeana.cloud.contentserviceapi.rest;

import static eu.europeana.cloud.contentserviceapi.rest.PathConstants.P_PROVIDER;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.ProviderNotExistsException;
import eu.europeana.cloud.contentserviceapi.service.DataProviderService;
import eu.europeana.cloud.definitions.model.DataProvider;

/**
 * Resource for DataProviders
 *
 */
@Path("/data-providers/{ID}")
@Component
public class DataProviderResource {

    @Autowired
    DataProviderService providerService;
    
    @PathParam("ID")
    private String providerId;
   
    
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public DataProvider getProvider() throws ProviderNotExistsException {
        return providerService.getProvider(providerId);
    }
    
    //add parameter for provider properties
    @PUT
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public DataProvider createProvider() {
        return providerService.createProvider(providerId);
    }
    

    @DELETE
    public void deleteProvider() throws ProviderNotExistsException {
        providerService.deleteProvider(providerId);
    }
    
}
