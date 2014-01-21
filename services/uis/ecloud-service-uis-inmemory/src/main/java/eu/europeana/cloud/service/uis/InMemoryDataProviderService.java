package eu.europeana.cloud.service.uis;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.dao.InMemoryDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProviderService implements DataProviderService {


    private InMemoryDataProviderDAO dataProviderDAO;


    /**
     * 
     * Creates a new instance of this class.
     * @param dataProviderDAO
     */
    public InMemoryDataProviderService(InMemoryDataProviderDAO dataProviderDAO){
    	this.dataProviderDAO = dataProviderDAO;
    }
    
    @Override
    public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
        if (thresholdProviderId != null) {
            throw new UnsupportedOperationException("Paging with threshold provider id is not supported");
        }
        List<DataProvider> providers = dataProviderDAO.getProviders(limit);
        return new ResultSlice<DataProvider>(null, providers);
    }


    @Override
    public DataProvider getProvider(String id)
            throws ProviderDoesNotExistException {
        return dataProviderDAO.getProvider(id);
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties)
            throws ProviderAlreadyExistsException {
        return dataProviderDAO.createProvider(providerId, properties);
    }



    @Override
    public DataProvider updateProvider(String providerId, DataProviderProperties properties)
            throws ProviderDoesNotExistException {
        return dataProviderDAO.updateProvider(providerId, properties);
    }
}
