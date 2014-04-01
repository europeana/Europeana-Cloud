package eu.europeana.cloud.service.uis;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	@Autowired
	private InMemoryDataProviderDAO dataProviderDAO;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryDataProviderService.class);

	/**
	 * Creates a new instance of this class.
	 */
	public InMemoryDataProviderService() {
		// nothing to do
	}

	/**
	 * 
	 * Creates a new instance of this class.
	 * 
	 * @param dataProviderDAO
	 */
	public InMemoryDataProviderService(InMemoryDataProviderDAO dataProviderDAO) {
        LOGGER.info("InMemoryDataProviderService starting...");
		this.dataProviderDAO = dataProviderDAO;
        LOGGER.info("InMemoryDataProviderService started successfully.");
	}

	@Override
	public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
        LOGGER.info("getProviders() providerId='{}', limit='{}'", thresholdProviderId, limit);
		if (thresholdProviderId != null) {
			throw new UnsupportedOperationException("Paging with threshold provider id is not supported");
		}
		List<DataProvider> providers = dataProviderDAO.getProviders(limit);
		return new ResultSlice<DataProvider>(null, providers);
	}

	@Override
	public DataProvider getProvider(String providerId) throws ProviderDoesNotExistException {
        LOGGER.info("getProvider() providerId='{}'", providerId);
		return dataProviderDAO.getProvider(providerId);
	}

	@Override
	public DataProvider createProvider(String providerId, DataProviderProperties properties)
			throws ProviderAlreadyExistsException {
        LOGGER.info("createProvider() providerId='{}', properties='{}'", providerId, properties);
		return dataProviderDAO.createProvider(providerId, properties);
	}

	@Override
	public DataProvider updateProvider(String providerId, DataProviderProperties properties)
			throws ProviderDoesNotExistException {
        LOGGER.info("updateProvider() providerId='{}', properties='{}'", providerId, properties);
		return dataProviderDAO.updateProvider(providerId, properties);
	}
}
