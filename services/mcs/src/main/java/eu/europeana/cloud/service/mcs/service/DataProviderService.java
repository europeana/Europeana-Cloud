package eu.europeana.cloud.service.mcs.service;

import java.util.List;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * DataProviderService
 *
 */
public interface DataProviderService {
    
    List<DataProvider> getProviders();
    
    DataProvider getProvider(String providerId) throws ProviderNotExistsException;
    
    DataProvider createProvider(String providerId); //TODO add parameter for provider configuration
    
    void deleteProvider(String providerId) throws ProviderNotExistsException;
    
    DataSet createDataSet(String providerId, String dataSetId) throws ProviderNotExistsException, DataSetAlreadyExistsException;

    List<DataSet> getDataSets(String providerId) throws ProviderNotExistsException;

    public void deleteDataSet(String providerId, String dataSetId) throws ProviderNotExistsException, DataSetNotExistsException;
}
