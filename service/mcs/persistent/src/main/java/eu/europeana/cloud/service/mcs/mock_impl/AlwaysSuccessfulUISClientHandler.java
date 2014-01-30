package eu.europeana.cloud.service.mcs.mock_impl;

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
     * @return true
     */
    @Override
    public boolean providerExistsInUIS(String providerId) {
        return true;
    }
}
