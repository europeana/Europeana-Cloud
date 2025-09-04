package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_PERMISSIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_RESOURCE;

import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.LogMessageCleaner;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetDeletionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import java.util.Arrays;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource to manage data sets.
 */
@RestController
public class DataSetResource {

  private static final String DATASET_CLASS_NAME = DataSet.class.getName();
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSetResource.class);

  private static final List<String> ACCEPTED_PERMISSION_VALUES = Arrays.asList(
      eu.europeana.cloud.common.model.Permission.ALL.getValue(),
      eu.europeana.cloud.common.model.Permission.READ.getValue(),
      eu.europeana.cloud.common.model.Permission.WRITE.getValue(),
      eu.europeana.cloud.common.model.Permission.ADMINISTRATION.getValue(),
      eu.europeana.cloud.common.model.Permission.DELETE.getValue()
  );

  private static final String DATASET_PERMISSION_KEY = "eu.europeana.cloud.common.model.DataSet";

  private final DataSetService dataSetService;

  private final MutableAclService mutableAclService;

  private final PermissionsGrantingManager permissionsGrantingManager;

  @Value("${numberOfElementsOnPage}")
  private int numberOfElementsOnPage;

  public DataSetResource(
      DataSetService dataSetService,
      MutableAclService mutableAclService,
      PermissionsGrantingManager permissionsGrantingManager) {
    this.dataSetService = dataSetService;
    this.mutableAclService = mutableAclService;
    this.permissionsGrantingManager = permissionsGrantingManager;
  }

  /**
   * Deletes data set.
   * <strong>Delete permissions required.</strong>
   *
   * @param providerId identifier of the dataset's provider(required).
   * @param dataSetId identifier of the deleted data set(required).
   * @throws DataSetNotExistsException data set not exists.
   */
  @DeleteMapping(DATA_SET_RESOURCE)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', delete)")
  public void deleteDataSet(
      @PathVariable("providerId") String providerId,
      @PathVariable("dataSetId") String dataSetId) throws DataSetDeletionException, DataSetNotExistsException {

    dataSetService.deleteDataSet(providerId, dataSetId);

    // let's delete the permissions as well
    String ownersName = SpringUserUtils.getUsername();
    if (ownersName != null) {
      ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(DATASET_CLASS_NAME,
          dataSetId + "/" + providerId);
      mutableAclService.deleteAcl(dataSetIdentity, false);
    }
  }

  /**
   * Lists representation versions from data set. Result is returned in slices.
   *
   * @param providerId identifier of the dataset's provider (required).
   * @param dataSetId identifier of a data set (required).
   * @param startFrom reference to next slice of result. If not provided, first slice of result will be returned.
   * @param existingOnly if set to true, records with deleted flag would be filtered out
   * @return slice of representation version list.
   * @throws DataSetNotExistsException no such data set exists.
   * @summary get representation versions from a data set
   */
  @GetMapping(value = DATA_SET_RESOURCE, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public ResultSlice<Representation> getDataSetContents(
      HttpServletRequest httpServletRequest,
      @PathVariable("providerId") String providerId,
      @PathVariable("dataSetId") String dataSetId,
      @RequestParam(value = "existingOnly", defaultValue = "false") boolean existingOnly,
      @RequestParam(value = "startFrom", required = false) String startFrom) throws DataSetNotExistsException {

    ResultSlice<Representation> representations =
            dataSetService.listDataSet(providerId, dataSetId, startFrom, existingOnly, numberOfElementsOnPage);

    for (Representation rep : representations.getResults()) {
      EnrichUriUtil.enrich(httpServletRequest, rep);
    }
    return representations;
  }

  @RequestMapping(value = DATA_SET_RESOURCE, method = RequestMethod.HEAD)
  public void checkIfDatasetExists(@PathVariable String dataSetId, @PathVariable String providerId)
      throws DataSetNotExistsException {
    dataSetService.checkIfDatasetExists(dataSetId, providerId);
  }

  /**
   * Updates description of a data set.
   * <p>
   * <strong>Write permissions required.</strong>
   *
   * @param providerId identifier of the dataset's provider (required).
   * @param dataSetId identifier of a data set (required).
   * @param description description of data set
   * @throws DataSetNotExistsException no such data set exists.
   * @throws AccessDeniedOrObjectDoesNotExistException there is an attempt to access a resource without the proper permissions. or
   * the resource does not exist at all
   * @statuscode 204 object has been updated.
   */
  @PutMapping(DATA_SET_RESOURCE)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
  public void updateDataSet(
      @PathVariable String providerId,
      @PathVariable String dataSetId,
      @RequestParam String description) throws DataSetNotExistsException {

    dataSetService.updateDataSet(providerId, dataSetId, description);
  }

  @PutMapping(DATA_SET_PERMISSIONS_RESOURCE)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)" +
      "or hasRole('ROLE_ADMIN')")
  public void updateDataSetPermissionsForUser(
      @PathVariable String providerId,
      @PathVariable String dataSetId,
      @RequestParam String permission,
      @RequestParam String username
  ) {
    ParamUtil.validate("permission", permission, ACCEPTED_PERMISSION_VALUES);

    eu.europeana.cloud.common.model.Permission selectedPermission =
        eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());
    List<Permission> permissionsToBeUpdated = Arrays.asList(selectedPermission.getSpringPermissions());
    permissionsGrantingManager.grantPermissions(DATASET_PERMISSION_KEY, dataSetId + "/" + providerId, username,
        permissionsToBeUpdated);
  }

  @DeleteMapping(DATA_SET_PERMISSIONS_RESOURCE)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)" +
      "or hasRole('ROLE_ADMIN')")
  public ResponseEntity<String> removeDataSetPermissionsForUser(
      @PathVariable String providerId,
      @PathVariable String dataSetId,
      @RequestParam String permission,
      @RequestParam String username
  ) {
    ParamUtil.validate("permission", permission, ACCEPTED_PERMISSION_VALUES);

    eu.europeana.cloud.common.model.Permission selectedPermission =
        eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());

    ObjectIdentity versionIdentity = new ObjectIdentityImpl(DATASET_PERMISSION_KEY,
        dataSetId + "/" + providerId);

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Removing privileges for user '{}' to  '{}' with key '{}'",
          LogMessageCleaner.clean(username),
          versionIdentity.getType(),
          versionIdentity.getIdentifier());
    }

    List<Permission> permissionsToBeRemoved = Arrays.asList(selectedPermission.getSpringPermissions());
    permissionsGrantingManager.removePermissions(versionIdentity, username, permissionsToBeRemoved);

    return ResponseEntity.noContent().build();
  }
}
