package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION_PERSIST;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource to manage representation versions.
 */
@RestController
public class RepresentationVersionResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationVersionResource.class.getName());

  private final RecordService recordService;
  private final DataSetPermissionsVerifier dataSetPermissionsVerifier;

  public RepresentationVersionResource(
      RecordService recordService,
      DataSetPermissionsVerifier dataSetPermissionsVerifier) {
    this.recordService = recordService;
    this.dataSetPermissionsVerifier = dataSetPermissionsVerifier;
  }

  /**
   * Returns representation in a specified version.
   * <strong>Read permissions required.</strong>
   *
   * @param cloudId cloud id of the record which contains the representation(required).
   * @param representationName name of the representation(required).
   * @param version a specific version of the representation(required).
   * @return representation in requested version
   * @throws RepresentationNotExistsException representation does not exist in the specified version.
   * @summary get representation by version
   */
  @GetMapping(value = REPRESENTATION_VERSION, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  @PreAuthorize("isAuthenticated()")
  public Representation getRepresentationVersion(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("version") String version) throws RepresentationNotExistsException {

    Representation representation = recordService.getRepresentation(cloudId, representationName, version);
    EnrichUriUtil.enrich(httpServletRequest, representation);
    return representation;
  }

  /**
   * Deletes representation version.
   * <strong>Delete permissions required.</strong>
   *
   * @param cloudId cloud id of the record which contains the representation version (required).
   * @param representationName name of the representation(required).
   * @param version a specific version of the representation(required).
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException representation in specified version is persistent and as such cannot be
   * removed.
   */
  @DeleteMapping(value = REPRESENTATION_VERSION)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void deleteRepresentation(
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("version") String version)
      throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
      AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Representation representation = Representation.fromFields(cloudId, representationName, version);

    if (dataSetPermissionsVerifier.isUserAllowedToDelete(representation)) {
      recordService.deleteRepresentation(cloudId, representationName, version);
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  /**
   * Persists temporary representation.
   * <p/>
   * <strong>Write permissions required.</strong>
   *
   * @param cloudId cloud id of the record which contains the representation version(required).
   * @param representationName name of the representation(required).
   * @param version a specific version of the representation(required).
   * @return URI to the persisted representation in content-location.
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException representation version is already persistent.
   * @throws CannotPersistEmptyRepresentationException representation version has no file attached and as such cannot be made
   * persistent.
   * @statuscode 201 representation is made persistent.
   */
  @PostMapping(value = REPRESENTATION_VERSION_PERSIST)
  public ResponseEntity<Void> persistRepresentation(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("version") String version) throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException,
      AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Representation representation = Representation.fromFields(cloudId, representationName, version);

    if (dataSetPermissionsVerifier.isUserAllowedToPersistRepresentation(representation)) {
      Representation persistentRepresentation = recordService.persistRepresentation(cloudId, representationName, version);
      EnrichUriUtil.enrich(httpServletRequest, persistentRepresentation);
      return ResponseEntity.created(persistentRepresentation.getUri()).build();
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

}
