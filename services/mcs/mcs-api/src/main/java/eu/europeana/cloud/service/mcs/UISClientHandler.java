package eu.europeana.cloud.service.mcs;

public interface UISClientHandler {

    /**
     * Checks if given cloudId exist in Unique Identifiers Service. Throws SystemException in case of UIS error.
     * 
     * @param cloudId
     *            cloud id
     * @return true if cloudId exists in UIS, false otherwise
     */
    boolean recordExistInUIS(String cloudId);

    boolean providerExistsInUIS(String providerId);
    
}
