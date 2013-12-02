package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Data provider service using Cassandra as database.
 */
@Service
public class CassandraDataProviderService implements DataProviderService {

	@Autowired
	private CassandraDataProviderDAO dataProviderDAO;

	@Autowired
	private CassandraDataSetDAO dataSetDAO;

	@Autowired
	private CassandraRecordDAO recordDAO;


	/**
	 * @inheritDoc
	 */
	@Override
	public ResultSlice<DataProvider> getProviders(String thresholdProviderId, int limit) {
		String nextProvider = null;
		List<DataProvider> providers = dataProviderDAO.getProviders(thresholdProviderId, limit + 1);
		if (providers.size() == limit + 1) {
			nextProvider = providers.get(limit).getId();
			providers.remove(limit);
		}
		return new ResultSlice(nextProvider, providers);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public DataProvider getProvider(String providerId)
			throws ProviderNotExistsException {
		DataProvider dp = dataProviderDAO.getProvider(providerId);
		if (dp == null) {
			throw new ProviderNotExistsException();
		} else {
			return dp;
		}
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public DataProvider createProvider(String providerId, DataProviderProperties properties)
			throws ProviderAlreadyExistsException {
		DataProvider dp = dataProviderDAO.getProvider(providerId);
		if (dp != null) {
			throw new ProviderAlreadyExistsException();
		}
		return dataProviderDAO.createOrUpdateProvider(providerId, properties);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public DataProvider updateProvider(String providerId, DataProviderProperties properties)
			throws ProviderNotExistsException {
		DataProvider dp = dataProviderDAO.getProvider(providerId);
		if (dp == null) {
			throw new ProviderNotExistsException();
		}
		return dataProviderDAO.createOrUpdateProvider(providerId, properties);
	}


	/**
	 * @inheritDoc
	 */
	@Override
	public void deleteProvider(String providerId)
			throws ProviderNotExistsException, ProviderHasDataSetsException, ProviderHasRecordsException {
		boolean providerHasDataSets = !dataSetDAO.getDataSets(providerId, null, 1).isEmpty();
		if (providerHasDataSets) {
			throw new ProviderHasDataSetsException();
		}
		boolean providerHasRepresentation = recordDAO.providerHasRepresentations(providerId);
		if (providerHasRepresentation) {
			throw new ProviderHasRecordsException();
		}
		dataProviderDAO.deleteProvider(providerId);
	}
}
