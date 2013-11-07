package eu.europeana.cloud.service.mcs.persistent;

import java.util.List;
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


    @Override
    public List<DataProvider> getProviders() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public DataProvider getProvider(String providerId)
            throws ProviderNotExistsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
