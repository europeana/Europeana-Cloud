package eu.europeana.cloud.service.mcs.persistent;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * CassandraDataProviderService
 */
@Service
public class CassandraDataProviderService implements DataProviderService {

    @Autowired
    private CassandraDataProviderDAO dataProviderDAO;


    @Override
    public List<DataProvider> getProviders() {
        return dataProviderDAO.getProviders();
    }


    @Override
    public DataProvider getProvider(String providerId)
            throws ProviderNotExistsException {
        return dataProviderDAO.getProvider(providerId);
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties) {
        return dataProviderDAO.createOrUpdateProvider(providerId, properties);
    }


    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        dataProviderDAO.deleteProvider(providerId);
    }
}
