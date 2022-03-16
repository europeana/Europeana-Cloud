package eu.europeana.cloud.service.uis.service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Data provider service using Cassandra as database.
 */
public class CassandraDataProviderService implements DataProviderService {

    private CassandraDataProviderDAO dataProviderDao;

    public CassandraDataProviderService(CassandraDataProviderDAO dataProviderDao) {
        this.dataProviderDao = dataProviderDao;
    }

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
        return new ResultSlice<>(nextProvider, providers);
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
        return dataProviderDao.createDataProvider(providerId, properties);
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
        }else{
            dp.setProperties(properties);
            return dataProviderDao.updateDataProvider(dp);
        }
    }

    @Override
    public DataProvider updateProvider(DataProvider dataProvider) throws ProviderDoesNotExistException {
        LOGGER.info("updating data provider providerId='{}', properties='{}'", dataProvider.getId(), dataProvider.getProperties());
        DataProvider dp = dataProviderDao.getProvider(dataProvider.getId());
        if (dp == null) {
            LOGGER.warn("ProviderDoesNotExistException providerId='{}', properties='{}'", dataProvider.getId(), dataProvider.getProperties());
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(dataProvider.getId())));
        }
        return dataProviderDao.updateDataProvider(dataProvider);
    }

    @Override
    public void deleteProvider(String providerId) throws ProviderDoesNotExistException {
        LOGGER.info("Deleting provider {}", providerId);
        DataProvider dp = dataProviderDao.getProvider(providerId);
        if (dp == null) {
            LOGGER.warn("ProviderDoesNotExistException providerId='{}'", providerId);
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        dataProviderDao.deleteProvider(providerId);
    }
}
