package eu.europeana.cloud.service.mcs.service.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.service.DataProviderService;
import java.util.Collection;
import java.util.Iterator;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProvidersService implements DataProviderService {

    private Map<String, DataProvider> providers = new HashMap<>();
    private Map<String, Map<String, DataSet>> dataSets = new HashMap<>();
     
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
        dataSets.put(providerId, new HashMap<String, DataSet>());
        return provider;
    }

    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException {
        if(!providers.containsKey(providerId))
            throw new ProviderNotExistsException();
        providers.remove(providerId);
    }
    
        @Override
    public DataSet createDataSet(String providerId, String dataSetId) throws ProviderNotExistsException, DataSetAlreadyExistsException {
        if(!providers.containsKey(providerId))
            throw new ProviderNotExistsException();
        Map<String, DataSet> providerSets = dataSets.get(providerId);
        if (providerSets.containsKey(dataSetId)) {
            throw new DataSetAlreadyExistsException();
        }
        DataSet dataSet = new DataSet();
        dataSet.setId(dataSetId);
        dataSet.setProviderId(providerId);
        providerSets.put(dataSetId, dataSet);
        return dataSet;
    }

    @Override
    public List<DataSet> getDataSets(String providerId) throws ProviderNotExistsException {
        if (!providers.containsKey(providerId)) {
            throw new ProviderNotExistsException();
        }
        List<DataSet> sets = new ArrayList<>();
        Collection<Map<String, DataSet>> values = dataSets.values();
        Iterator<Map<String, DataSet>> iterator = values.iterator();
        while (iterator.hasNext()) {
            sets.addAll(iterator.next().values());
        }
        return sets;
    }

    @Override
    public void deleteDataSet(String providerId, String dataSetId) throws ProviderNotExistsException, DataSetNotExistsException {
        if (!providers.containsKey(providerId)) {
            throw new ProviderNotExistsException();
        }
        if(!dataSets.get(providerId).containsKey(dataSetId)){
            throw new DataSetNotExistsException();
        }
        dataSets.get(providerId).remove(dataSetId);
    }
}
