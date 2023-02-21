package eu.europeana.cloud.service.mcs;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;

/**
 * Interface for MCS->UIS communication.
 */
public interface UISClientHandler {

  /**
   * Checks if given cloudId exist in Unique Identifiers Service. Throws SystemException in case of UIS error.
   *
   * @param cloudId cloud id
   * @return true if cloudId exists in UIS, false otherwise
   */
  boolean existsCloudId(String cloudId);

  CloudId getCloudIdFromProviderAndLocalId(String providerId, String localId)
      throws ProviderNotExistsException, RecordNotExistsException;

  /**
   * Checks if provider with given id exist in Unique Identifiers Service. Throws SystemException in case of UIS error.
   *
   * @param providerId provider identifier
   * @return DataProvider from UIS
   */
  boolean existsProvider(String providerId);

  /**
   * Checks if provider with given id exist in Unique Identifiers Service. Throws SystemException in case of UIS error.
   *
   * @param providerId provider identifier
   * @return DataProvider from UIS
   */
  DataProvider getProvider(String providerId);

}
