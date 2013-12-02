package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * Service for data provider operations.
 *
 */
public interface DataProviderService {

	/**
	 * Returns all providers (in slices).
	 *
	 * @param thresholdProviderId if null - will return first result slice. Result slices contain token for next pages,
	 * which should be provided in this parameter.
	 * @param limit max number of results in one slice.
	 * @return list of providers as a result slice.
	 */
	ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit);


	/**
	 * Returns data provider for its id.
	 *
	 * @param providerId provider id.
	 * @return data provider with given id.
	 * @throws ProviderNotExistsException if threre is no provider with such id.
	 */
	DataProvider getProvider(String providerId)
			throws ProviderNotExistsException;


	/**
	 * Creates a new data provider.
	 *
	 * @param providerId id of provider.
	 * @param properties properties of provider.
	 * @return created provider.
	 */
	DataProvider createProvider(String providerId, DataProviderProperties properties)
			throws ProviderAlreadyExistsException;


	/**
	 * Updates properties for a data provider.
	 *
	 * @param providerId id of provider.
	 * @param properties new properties of provider.
	 * @return updated provider.
	 */
	DataProvider updateProvider(String providerId, DataProviderProperties properties)
			throws ProviderNotExistsException;


	/**
	 * Deletes data provider. Data provider must not have any data sets or record representations to be deleted.
	 *
	 * @param providerId
	 * @throws ProviderNotExistsException if such data provider not exists.
	 * @throws ProviderHasDataSetsException if provider cannot be deleted because it has data sets.
	 * @throws ProviderHasRecordsException if provider cannot be deleted because it has record representations.
	 */
	void deleteProvider(String providerId)
			throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException;
}
