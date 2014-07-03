package eu.europeana.cloud.service.mcs.mock_impl;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;

/**
 * Mock of UISClientHandler which returns true for all requests.
 */
public class AlwaysSuccessfulUISClientHandler implements UISClientHandler {

    /**
     * @inheritDoc
     * @return true
     */
    @Override
    public boolean recordExistInUIS(String cloudId) {
	return true;
    }

    /**
     * @inheritDoc
     * @return new DataProvider
     */
    @Override
    public DataProvider providerExistsInUIS(String providerId) {
	return new DataProvider();
    }
}
