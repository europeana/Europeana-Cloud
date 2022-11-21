package eu.europeana.cloud.service.mcs.mock_impl;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

/**
 * Mock of UISClientHandler which returns true for all requests.
 */
public class AlwaysSuccessfulUISClientHandler implements UISClientHandler {

  @Override
  public boolean existsCloudId(String cloudId) {
    return true;
  }

  @Override
  public CloudId getCloudIdFromProviderAndLocalId(String providerId, String localId) throws ProviderNotExistsException {
    return new CloudId();
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
