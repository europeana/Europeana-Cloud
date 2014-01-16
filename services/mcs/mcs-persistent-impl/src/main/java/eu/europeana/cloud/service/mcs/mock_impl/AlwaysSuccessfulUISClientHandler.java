package eu.europeana.cloud.service.mcs.mock_impl;

import eu.europeana.cloud.service.mcs.UISClientHandler;

public class AlwaysSuccessfulUISClientHandler implements UISClientHandler {

    /**
     * Mock of UISClientHandler which always returns true.
     * 
     * @param cloudId
     * @return true
     */
    @Override
    public boolean recordExistInUIS(String cloudId) {
        return true;
    }


    @Override
    public boolean providerExistsInUIS(String providerId) {
        return true;
    }
}
