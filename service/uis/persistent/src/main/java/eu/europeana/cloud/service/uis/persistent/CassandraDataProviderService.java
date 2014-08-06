package eu.europeana.cloud.service.uis.persistent;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * Data provider service using Cassandra as database.
 */
@Service
public class CassandraDataProviderService implements DataProviderService {

    @Autowired
    private CassandraDataProviderDAO dataProviderDao;
    
	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataProviderService.class);

    @Override
    public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
        LOGGER.info("getProviders() thresholdProviderId='{}', limit='{}'", thresholdProviderId, limit);
        String nextProvider = null;
        List<DataProvider> providers = dataProviderDao.getProviders(thresholdProviderId, limit + 1);
        final int providerSize = providers.size();
        if (providerSize == limit + 1) {
            nextProvider = providers.get(limit).getId();
            providers.remove(limit);
        }
        LOGGER.info("getProviders() returning providers={} and nextProvider={} for thresholdProviderId='{}', limit='{}'", 
        		providerSize, nextProvider, thresholdProviderId, limit);
        return new ResultSlice<DataProvider>(nextProvider, providers);
    }


    @Override
    public DataProvider getProvider(String providerId)
            throws ProviderDoesNotExistException {
        LOGGER.info("getProvider() providerId='{}'", providerId);
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp == null) {
	        LOGGER.warn("ProviderDoesNotExistException providerId='{}''", providerId);
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
        LOGGER.info("createProvider() providerId='{}', properties='{}'", providerId, properties);
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp != null) {
	        LOGGER.warn("ProviderAlreadyExistsException providerId='{}', properties='{}'", providerId, properties);
        	throw new ProviderAlreadyExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getErrorInfo(providerId)));
        }
        return dataProviderDao.createOrUpdateProvider(providerId, properties);
    }


    @Override
    public DataProvider updateProvider(String providerId, DataProviderProperties properties)
            throws ProviderDoesNotExistException {
        LOGGER.info("updateProvider() providerId='{}', properties='{}'", providerId, properties);
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp == null) {
	        LOGGER.warn("ProviderDoesNotExistException providerId='{}', properties='{}'", providerId, properties);
        	throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        return dataProviderDao.createOrUpdateProvider(providerId, properties);
    }


}
