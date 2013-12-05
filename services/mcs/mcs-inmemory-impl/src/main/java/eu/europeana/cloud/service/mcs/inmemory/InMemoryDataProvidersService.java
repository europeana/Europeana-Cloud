package eu.europeana.cloud.service.mcs.inmemory;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProvidersService implements DataProviderService {

    @Autowired
    private InMemoryDataSetDAO dataSetDAO;

    @Autowired
    private InMemoryDataProviderDAO dataProviderDAO;


    @Override
    public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
        if (thresholdProviderId != null) {
            throw new UnsupportedOperationException("Paging with threshold provider id is not supported");
        }
        List<DataProvider> providers = dataProviderDAO.getProviders(limit);
        return new ResultSlice(null, providers);
    }


    @Override
    public DataProvider getProvider(String id)
            throws ProviderNotExistsException {
        return dataProviderDAO.getProvider(id);
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties)
            throws ProviderAlreadyExistsException {
        return dataProviderDAO.createProvider(providerId, properties);
    }


    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        List<DataSet> providerDataSets = dataSetDAO.getDataSets(providerId);
        if (providerDataSets != null && !providerDataSets.isEmpty()) {
            throw new ProviderHasDataSetsException();
        }
        dataProviderDAO.deleteProvider(providerId);
    }


    @Override
    public DataProvider updateProvider(String providerId, DataProviderProperties properties)
            throws ProviderNotExistsException {
        return dataProviderDAO.updateProvider(providerId, properties);
    }
}
