package eu.europeana.cloud.service.mcs;

/**
 * Interface for MCS->UIS communication.
 */
public interface UISClientHandler {

    /**
     * Checks if given cloudId exist in Unique Identifiers Service. Throws SystemException in case of UIS error.
     * 
     * @param cloudId
     *            cloud id
     * @return true if cloudId exists in UIS, false otherwise
     */
    boolean recordExistInUIS(String cloudId);


    /**
     * Checks if provider with given id exist in Unique Identifiers Service. Throws SystemException in case of UIS
     * error.
     * 
     * @param providerId
     *            provider identifier
     * @return true if provider exists in UIS, false otherwise
     */
    boolean providerExistsInUIS(String providerId);

}
