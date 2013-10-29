package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.DataProviderService;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.DataSetService;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProvidersService implements DataProviderService {

    private Map<String, DataProvider> providers = new HashMap<>();
    
    private DataSetService dataSetService;


    @Override
    public List<DataProvider> getProviders() {
        return new ArrayList<>(providers.values());
    }


    @Override
    public DataProvider getProvider(String id)
            throws ProviderNotExistsException {
        DataProvider provider = providers.get(id);
        if (provider == null) {
            throw new ProviderNotExistsException();
        }
        return provider;
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties) {
        DataProvider provider = new DataProvider();
        provider.setId(providerId);
        provider.setProperties(properties);
        providers.put(providerId, provider);
        return provider;
    }


    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException,
            ProviderHasDataSetsException, ProviderHasRecordsException {
        if (!providers.containsKey(providerId)) {
            throw new ProviderNotExistsException();
        }
        List<DataSet> providerDataSets = dataSetService.getDataSets(providerId);
        if (providerDataSets != null && !providerDataSets.isEmpty()) {
            throw new ProviderHasDataSetsException();
        }
        providers.remove(providerId);
    }
}
