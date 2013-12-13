package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.persistent.UISClientHandler;

public class AlwaysSuccessfulUISClientHandler implements UISClientHandler {

    @Override
    public boolean recordExistInUIS(String cloudId) {
        return true;
    }
}
