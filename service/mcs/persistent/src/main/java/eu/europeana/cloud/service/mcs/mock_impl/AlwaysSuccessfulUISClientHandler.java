package eu.europeana.cloud.service.mcs.mock_impl;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;

/**
 * Mock of UISClientHandler which returns true for all requests.
 */
public class AlwaysSuccessfulUISClientHandler implements UISClientHandler {

    @Override
    public boolean existsCloudId(String cloudId) {
        return true;
    }

    @Override
    public boolean existsProvider(String cloudId) {
        return true;
    }

    @Override
    public DataProvider getProvider(String providerId) {
        return new DataProvider();
    }
}
