package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * InMemoryDataProviderDAO
 */
public class InMemoryDataProviderDAO {

    private Map<String, DataProvider> providers = new HashMap<>();


    public List<DataProvider> getProviders() {
        return new ArrayList<>(providers.values());
    }


    public DataProvider getProvider(String id)
            throws ProviderNotExistsException {
        DataProvider provider = providers.get(id);
        if (provider == null) {
            throw new ProviderNotExistsException();
        }
        return provider;
    }


    public DataProvider createProvider(String providerId, DataProviderProperties properties) {
        if (providers.containsKey(providerId)) {
            throw new ProviderAlreadyExistsException();
        }
        DataProvider provider = new DataProvider();
        provider.setId(providerId);
        provider.setProperties(properties);
        providers.put(providerId, provider);
        return provider;
    }


    public void deleteProvider(String providerId)
            throws ProviderNotExistsException,
            ProviderHasDataSetsException, ProviderHasRecordsException {
        if (!providers.containsKey(providerId)) {
            throw new ProviderNotExistsException();
        }

        providers.remove(providerId);
    }
}
