package eu.europeana.cloud.contentserviceapi.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.contentserviceapi.exception.ProviderNotExistsException;
import eu.europeana.cloud.contentserviceapi.service.DataProviderService;
import eu.europeana.cloud.definitions.model.DataProvider;
import eu.europeana.cloud.definitions.model.Provider;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProvidersServiceImpl implements DataProviderService {

    private Map<String, DataProvider> providers = new HashMap<>();
    
    @Override
    public List<DataProvider> getProviders() {
        return new ArrayList<>(providers.values());
    }

    @Override
    public DataProvider getProvider(String id) throws ProviderNotExistsException {
        DataProvider provider = providers.get(id);
        if(provider==null)
            throw new ProviderNotExistsException();
        return provider;
    }

    @Override
    public DataProvider createProvider(String providerId) {
        DataProvider provider = new DataProvider();
        provider.setId(providerId);
        providers.put(providerId, provider);
        return provider;
    }

    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException {
        if(!providers.containsKey(providerId))
            throw new ProviderNotExistsException();
        providers.remove(providerId);
    }
}
