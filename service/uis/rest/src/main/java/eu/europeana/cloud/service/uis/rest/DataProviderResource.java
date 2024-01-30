package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.RestInterfaceConstants;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource for DataProvider.
 */
@RestController
public class DataProviderResource {

  protected static final String LOCAL_ID_CLASS_NAME = "LocalId";
  private final UniqueIdentifierService uniqueIdentifierService;
  private final DataProviderService providerService;
  private final ACLServiceWrapper aclWrapper;

  public DataProviderResource(UniqueIdentifierService uniqueIdentifierService,
      DataProviderService providerService,
      ACLServiceWrapper aclWrapper) {
    this.uniqueIdentifierService = uniqueIdentifierService;
    this.providerService = providerService;
    this.aclWrapper = aclWrapper;
  }


  /**
   * Retrieves details about selected data provider
   *
   * @param providerId <strong>REQUIRED</strong> identifier of the provider that will be retrieved
   * @return Selected Data provider details
   * @throws ProviderDoesNotExistException The supplied provider does not exist
   */
  @GetMapping(value = RestInterfaceConstants.DATA_PROVIDER, produces = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  public DataProvider getProvider(@PathVariable("providerId") String providerId) throws ProviderDoesNotExistException {
    return providerService.getProvider(providerId);
  }

  /**
   * Updates data provider information.
   * <p>
   * <br/> <br/> <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'> <strong>Required
   * permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * <li>Write permission for the selected data provider</li>
   * </ul>
   * </div>
   *
   * @param dataProviderProperties <strong>REQUIRED</strong> data provider properties.
   * @param providerId <strong>REQUIRED</strong> identifier of data provider which will be updated.
   * @throws ProviderDoesNotExistException The supplied provider does not exist
   * @statuscode 204 object has been updated.
   */
  @PutMapping(value = RestInterfaceConstants.DATA_PROVIDER, consumes = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public ResponseEntity<Void> updateProvider(
      @RequestBody DataProviderProperties dataProviderProperties,
      @PathVariable("providerId") String providerId) throws ProviderDoesNotExistException {

    providerService.updateProvider(providerId, dataProviderProperties);
    return ResponseEntity.noContent().build();
  }

  /**
   * Create a mapping between a cloud identifier and a record identifier for a provider
   * <p>
   * <br/> <br/> <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'> <strong>Required
   * permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * </ul>
   * </div>
   *
   * @param providerId <strong>REQUIRED</strong> identifier of data provider, owner of the record
   * @param cloudId <strong>REQUIRED</strong> cloud identifier for which new record identifier will be added
   * @param recordId record identifier which will be bound to selected cloud identifier. If not specified, random one will be
   * generated
   * @return The newly associated cloud identifier
   * @throws DatabaseConnectionException database connection error
   * @throws CloudIdDoesNotExistException cloud identifier does not exist
   * @throws ProviderDoesNotExistException provider does not exist
   * @throws RecordDatasetEmptyException empty dataset
   * @throws CloudIdAlreadyExistException cloud identifier already exist
   */
  @PostMapping(value = RestInterfaceConstants.CLOUD_ID_TO_RECORD_ID_MAPPING, produces = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("isAuthenticated()")
  public ResponseEntity<CloudId> createIdMapping(
      @PathVariable("providerId") String providerId,
      @PathVariable("cloudId") String cloudId,
      @RequestParam(value = "recordId", required = false) String recordId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException,
      ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException {

    CloudId result;
    if (recordId != null) {
      result = uniqueIdentifierService.createIdMapping(cloudId, providerId, recordId);
    } else {
      result = uniqueIdentifierService.createIdMapping(cloudId, providerId);
    }

    return ResponseEntity.ok(result);
  }

}
