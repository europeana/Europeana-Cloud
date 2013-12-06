package eu.europeana.cloud.service.mcs.inmemory;

import java.util.*;

import org.springframework.stereotype.Repository;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * InMemoryDataProviderDAO
 */
@Repository
public class InMemoryDataProviderDAO {

    private Map<String, DataProvider> providers = new HashMap<>();


    public List<DataProvider> getProviders(int limit) {
        Collection<DataProvider> providerList = providers.values();
        return new ArrayList<>(providerList).subList(0, Math.min(limit, providerList.size()));
    }


    public DataProvider getProvider(String id)
            throws ProviderNotExistsException {
        DataProvider provider = providers.get(id);
        if (provider == null) {
            throw new ProviderNotExistsException();
        }
        return provider;
    }


    public DataProvider createProvider(String providerId, DataProviderProperties properties)
            throws ProviderAlreadyExistsException {
        if (providers.containsKey(providerId)) {
            throw new ProviderAlreadyExistsException();
        }
        DataProvider provider = new DataProvider();
        provider.setId(providerId);
        provider.setProperties(properties);
        providers.put(providerId, provider);
        return provider;
    }


    public DataProvider updateProvider(String providerId, DataProviderProperties properties)
            throws ProviderNotExistsException {
        DataProvider dp = providers.get(providerId);
        if (dp == null) {
            throw new ProviderNotExistsException();
        }
        dp.setProperties(properties);
        return dp;
    }


    public void deleteProvider(String providerId)
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        if (!providers.containsKey(providerId)) {
            throw new ProviderNotExistsException();
        }

        providers.remove(providerId);
    }
}
