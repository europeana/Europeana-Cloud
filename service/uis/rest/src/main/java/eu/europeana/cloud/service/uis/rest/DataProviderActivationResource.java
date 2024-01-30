package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.RestInterfaceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(RestInterfaceConstants.DATA_PROVIDER_ACTIVATION)
public class DataProviderActivationResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataProviderActivationResource.class);

  private final DataProviderService providerService;

  public DataProviderActivationResource(DataProviderService providerService) {
    this.providerService = providerService;
  }

  /**
   * Activates data provider (sets flag 'active' to true)
   * <p>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   *      <strong>Required permissions:</strong>
   *      <ul>
   *          <li>Admin role</li>
   *      </ul>
   * </div>
   *
   * @param providerId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated
   * @return Empty response with http status code indicating whether the operation was successful or not
   * @throws ProviderDoesNotExistException Supplied Data-provider does not exist
   * @summary Data-provider activation
   * @author
   */
  @PutMapping
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public ResponseEntity<Void> activateDataProvider(@PathVariable String providerId) throws ProviderDoesNotExistException {
    LOGGER.info("Activating data provider: {}", providerId);

    DataProvider dataProvider = providerService.getProvider(providerId);
    dataProvider.setActive(true);
    providerService.updateProvider(dataProvider);

    return ResponseEntity.ok().build();
  }

  /**
   * Deactivates data provider (sets flag 'active' to false)
   * <p>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   *      <strong>Required permissions:</strong>
   *      <ul>
   *          <li>Admin role</li>
   *      </ul>
   * </div>
   *
   * @param providerId <strong>REQUIRED</strong> identifier of data-provider which is about to be activated
   * @return Empty response with http status code indicating whether the operation was successful or not
   * @throws ProviderDoesNotExistException Supplied Data-provider does not exist
   * @summary Data-provider deactivation
   * @author
   */
  @DeleteMapping
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public ResponseEntity<Void> deactivateDataProvider(@PathVariable("providerId") String providerId)
      throws ProviderDoesNotExistException {
    LOGGER.info("Deactivating data provider: {}", providerId);

    DataProvider dataProvider = providerService.getProvider(providerId);
    dataProvider.setActive(false);
    providerService.updateProvider(dataProvider);

    return ResponseEntity.ok().build();
  }

}
