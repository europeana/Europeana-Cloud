package eu.europeana.cloud.service.mcs.persistent;

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
}
