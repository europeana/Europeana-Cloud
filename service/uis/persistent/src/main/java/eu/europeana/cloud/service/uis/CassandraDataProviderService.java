package eu.europeana.cloud.service.uis;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.database.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * Data provider service using Cassandra as database.
 */
@Service
public class CassandraDataProviderService implements DataProviderService {

    @Autowired
    private CassandraDataProviderDAO dataProviderDao;

    @Override
    public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
        String nextProvider = null;
        List<DataProvider> providers = dataProviderDao.getProviders(thresholdProviderId, limit + 1);
        if (providers.size() == limit + 1) {
            nextProvider = providers.get(limit).getId();
            providers.remove(limit);
        }
        return new ResultSlice<DataProvider>(nextProvider, providers);
    }


    @Override
    public DataProvider getProvider(String providerId)
            throws ProviderDoesNotExistException {
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp == null) {
        	throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        } else {
            return dp;
        }
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties)
            throws ProviderAlreadyExistsException {
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp != null) {
        	throw new ProviderAlreadyExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getErrorInfo(providerId)));
        }
        return dataProviderDao.createOrUpdateProvider(providerId, properties);
    }


    @Override
    public DataProvider updateProvider(String providerId, DataProviderProperties properties)
            throws ProviderDoesNotExistException {
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp == null) {
        	throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        return dataProviderDao.createOrUpdateProvider(providerId, properties);
    }


}
