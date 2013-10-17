package eu.europeana.cloud.contentserviceapi.service;

import java.util.List;

import eu.europeana.cloud.contentserviceapi.exception.ProviderNotExistsException;
import eu.europeana.cloud.definitions.model.DataProvider;

/**
 * DataProviderService
 *
 */
public interface DataProviderService {
    
    List<DataProvider> getProviders();
    
    DataProvider getProvider(String providerId) throws ProviderNotExistsException;
    
    DataProvider createProvider(String providerId); //TODO add parameter for provider configuration
    
    void deleteProvider(String providerId) throws ProviderNotExistsException;
}
