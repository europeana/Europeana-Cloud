package eu.europeana.cloud.service.uis.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * InMemoryDataProviderDAO
 */
@Repository
public class InMemoryDataProviderDAO {

	private Map<String, DataProvider> providers = new HashMap<>();

	/**
	 * Get a predefined number of providers
	 * @param limit The number of providers to retrieve
	 * @return A List of data providers
	 */
	public List<DataProvider> getProviders(int limit) {
		Collection<DataProvider> providerList = providers.values();
		return new ArrayList<>(providerList).subList(0, Math.min(limit, providerList.size()));
	}

	/**
	 * Get a data provider with the selected id
	 * @param id The id to search for
	 * @return The data provider with the provided id
	 * @throws ProviderDoesNotExistException
	 */
	public DataProvider getProvider(String id) throws ProviderDoesNotExistException {
		DataProvider provider = providers.get(id);
		if (provider == null) {
			throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(id)));
		}
		return provider;
	}

	/**
	 * Create a new data provider
	 * @param providerId The data provider Id
	 * @param properties The properties of the data provider
	 * @return The newly created Data Provider
	 * @throws ProviderAlreadyExistsException
	 */
	public DataProvider createProvider(String providerId, DataProviderProperties properties)
			throws ProviderAlreadyExistsException {
		if (providers.containsKey(providerId)) {
			throw new ProviderAlreadyExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getErrorInfo(providerId)));
		}
		DataProvider provider = new DataProvider();
		provider.setId(providerId);
		provider.setProperties(properties);
		providers.put(providerId, provider);
		return provider;
	}

	/**
	 * Update a data provider 
	 * @param providerId The provider Id to search for
	 * @param properties The properties of the data provider to update
	 * @return The updated data provider
	 * @throws ProviderDoesNotExistException
	 */
	public DataProvider updateProvider(String providerId, DataProviderProperties properties)
			throws ProviderDoesNotExistException {
		DataProvider dp = providers.get(providerId);
		if (dp == null) {
			throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
		}
		dp.setProperties(properties);
		return dp;
	}

}
