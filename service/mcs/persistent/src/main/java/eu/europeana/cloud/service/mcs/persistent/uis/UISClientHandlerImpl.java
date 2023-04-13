package eu.europeana.cloud.service.mcs.persistent.uis;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import java.util.Iterator;

/**
 * Communicates with Unique Identifier Service using UISClient. Used for checking if cloudIds and providers exists in UIS.
 */
public class UISClientHandlerImpl implements UISClientHandler {

  private UISClient uisClient;

  public UISClientHandlerImpl(UISClient uisClient) {
    this.uisClient = uisClient;
  }

  /**
   * @inheritDoc
   */
  @Override
  public boolean existsCloudId(String cloudId) {
    boolean result = false;
    try {
      ResultSlice<CloudId> records = uisClient.getRecordId(cloudId);
      if (records == null) {
        throw new IllegalStateException("UIS returned null");
      }
      if (records.getResults().isEmpty()) {
        throw new IllegalStateException("UIS returned empty list");
      }

      Iterator<CloudId> iterator = records.getResults().iterator();
      while (iterator.hasNext()) {
        CloudId ci = iterator.next();
        if (ci.getId().equals(cloudId)) {
          result = true;
          break;
        }
      }
      if (!result) {
        throw new IllegalStateException(String.format(
            "Cloud id %s not on the list returned by UIS", cloudId));
      }
    } catch (CloudException ex) {
      if (ex.getCause() instanceof CloudIdDoesNotExistException) {
        result = false;
      } else {
        throw new SystemException(ex);
      }
    }
    return result;
  }

  @Override
  public CloudId getCloudIdFromProviderAndLocalId(String providerId, String localId)
      throws ProviderNotExistsException, RecordNotExistsException {
    try {
      return uisClient.getCloudId(providerId, localId);
    } catch (CloudException ex) {
      if (ex.getCause() instanceof RecordDoesNotExistException) {
        throw new RecordNotExistsException(localId, providerId);
      } else if (ex.getCause() instanceof ProviderDoesNotExistException) {
        throw new ProviderNotExistsException(ex.getMessage());
      } else {
        throw new SystemException(ex);
      }
    }
  }


  /**
   * @inheritDoc
   */
  @Override
  public boolean existsProvider(String providerId) {
    DataProvider result;
    try {
      result = uisClient.getDataProvider(providerId);
    } catch (CloudException e) {
      if (e.getCause() instanceof ProviderDoesNotExistException) {
        result = null;
      } else {
        throw new SystemException(e);
      }
    }
    return result != null;
  }

  /**
   * @inheritDoc
   */
  @Override
  public DataProvider getProvider(String providerId) {
    DataProvider result;
    try {
      result = uisClient.getDataProvider(providerId);
    } catch (CloudException e) {
      if (e.getCause() instanceof ProviderDoesNotExistException) {
        return null;
      } else {
        throw new SystemException(e);
      }
    }
    return result;
  }
}
