package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.RestInterfaceConstants;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Implementation of the Unique Identifier Service.
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@RestController
public class UniqueIdentifierResource {

  private static final String CLOUD_ID_CLASS_NAME = CloudId.class.getName();
  private final UniqueIdentifierService uniqueIdentifierService;
  private final DataProviderResource dataProviderResource;
  private final ACLServiceWrapper aclWrapper;


  public UniqueIdentifierResource(
      UniqueIdentifierService uniqueIdentifierService,
      DataProviderResource dataProviderResource,
      ACLServiceWrapper aclWrapper) {
    this.uniqueIdentifierService = uniqueIdentifierService;
    this.dataProviderResource = dataProviderResource;
    this.aclWrapper = aclWrapper;
  }

  /**
   * Invokes the generation of a cloud identifier using the provider identifier and a record identifier.
   * <p>
   * <br/> <br/> <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'> <strong>Required
   * permissions:</strong>
   * <ul>
   * <li>Authenticated user</li>
   * </ul>
   * </div>
   *
   * @param providerId <strong>REQUIRED</strong> identifier of data-provider for which new cloud identifier will be created.
   * @param recordId record identifier which will be binded to the newly created cloud identifier. If not provided, random value
   * will be generated.
   * @return The newly created CloudId
   * @throws DatabaseConnectionException database error
   * @throws RecordExistsException Record already exists in repository
   * @throws ProviderDoesNotExistException Supplied Data-provider does not exist
   * @throws RecordDatasetEmptyException dataset is empty
   * @throws CloudIdDoesNotExistException cloud identifier does not exist
   * @throws CloudIdAlreadyExistException Cloud identifier was created previously
   */
  @PostMapping(value = RestInterfaceConstants.CLOUD_IDS, produces = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("isAuthenticated()")
  public ResponseEntity<CloudId> createCloudId(
      @RequestParam("providerId") String providerId,
      @RequestParam(value = "recordId", required = false) String recordId)
      throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException,
      RecordDatasetEmptyException, CloudIdDoesNotExistException, CloudIdAlreadyExistException {

    final CloudId cId = (recordId != null) ? (uniqueIdentifierService.createCloudId(providerId, recordId))
        : (uniqueIdentifierService.createCloudId(providerId));

    return ResponseEntity.ok(cId);
  }

  /**
   * Retrieves cloud identifier based on given provider identifier and record identifier
   *
   * @param providerId <strong>REQUIRED</strong> provider identifier
   * @param recordId <strong>REQUIRED</strong> record identifier
   * @return Cloud identifier associated with given provider identifier and record identifier
   * @throws DatabaseConnectionException database error
   * @throws RecordDoesNotExistException record does not exist
   * @throws ProviderDoesNotExistException provider does not exist
   * @throws RecordDatasetEmptyException dataset is empty
   */
  @GetMapping(value = RestInterfaceConstants.CLOUD_IDS, produces = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<CloudId> getCloudId(@RequestParam("providerId") String providerId,
      @RequestParam("recordId") String recordId)
      throws DatabaseConnectionException, RecordDoesNotExistException, ProviderDoesNotExistException,
      RecordDatasetEmptyException {
    return ResponseEntity.ok(uniqueIdentifierService.getCloudId(providerId, recordId));
  }


  /**
   * Retrieves list of record Identifiers associated with the cloud identifier. Result is returned in slices which contain fixed
   * amount of results and reference (token) to next slice of results.
   *
   * @param cloudId <strong>REQUIRED</strong> cloud identifier for which list of all record identifiers will be retrieved
   * @return The list of record identifiers bound to given provider identifier
   * @throws DatabaseConnectionException database error
   * @throws CloudIdDoesNotExistException cloud identifier does not exist
   * @throws ProviderDoesNotExistException provider does not exist
   * @throws RecordDatasetEmptyException dataset is empty
   */
  @GetMapping(value = RestInterfaceConstants.CLOUD_ID, produces = {MediaType.APPLICATION_XML_VALUE,
      MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<ResultSlice<CloudId>> getLocalIds(@PathVariable("cloudId") String cloudId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException,
      RecordDatasetEmptyException {
    ResultSlice<CloudId> pList = new ResultSlice<>();
    pList.setResults(uniqueIdentifierService.getLocalIdsByCloudId(cloudId));
    return ResponseEntity.ok(pList);
  }

}
