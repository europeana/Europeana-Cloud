package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_ADD;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_ADD_WITH_PROVIDER;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_ADD_WITH_PROVIDER_TAG;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_DELETE;

import com.google.common.collect.Sets;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import jakarta.servlet.http.HttpServletRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Created by Tarek on 8/2/2016.
 */
@RestController
public class RevisionResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RevisionResource.class.getName());

  private final RecordService recordService;
  private final DataSetService dataSetService;
  private final DataSetPermissionsVerifier dataSetPermissionsVerifier;

  public RevisionResource(RecordService recordService, DataSetService dataSetService,
      DataSetPermissionsVerifier dataSetPermissionsVerifier) {
    this.recordService = recordService;
    this.dataSetService = dataSetService;
    this.dataSetPermissionsVerifier = dataSetPermissionsVerifier;
  }

  /**
   * Adds a new revision to representation version.
   * <strong>Read permissions required.</strong>
   *
   * @param cloudId cloud id of the record (required).
   * @param representationName schema of representation (required).
   * @param version a specific version of the representation(required).
   * @param revisionName the name of revision (required).
   * @param revisionProviderId revision provider id (required).
   * @param tag tag flag (deleted)
   * @return URI to specific revision with specific tag inside a version.
   * @throws RepresentationNotExistsException representation does not exist in specified version
   * @throws RevisionIsNotValidException if the added revision was not valid
   * @statuscode 201 object has been created.
   */
  @PostMapping(value = REVISION_ADD_WITH_PROVIDER_TAG)
  public ResponseEntity<String> addRevision(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") final String cloudId,
      @PathVariable("representationName") final String representationName,
      @PathVariable("version") final String version,
      @PathVariable("revisionName") String revisionName,
      @PathVariable("revisionProviderId") String revisionProviderId,
      @PathVariable("tag") String tag)
      throws RepresentationNotExistsException, RevisionIsNotValidException,
      AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    ParamUtil.validate("tag", tag,
        Arrays.asList(Tags.DELETED.getTag()));
    //
    Representation representation = Representation.fromFields(cloudId, representationName, version);
    //
    if (dataSetPermissionsVerifier.isUserAllowedToAddRevisionTo(representation)) {
      Revision revision = Revision.fromParams(revisionName, revisionProviderId, tag);
      addRevisionToRepresentationVersion(revision, representation);
      return createResponseEntity(httpServletRequest, null);
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  /**
   * Adds a new revision to representation version.
   * <strong>Read permissions required.</strong>
   *
   * @param revision Revision (required).
   * @return URI to revisions inside a version.
   * @throws RepresentationNotExistsException representation does not exist in specified version
   * @statuscode 201 object has been created.
   */
  @PostMapping(value = REVISION_ADD, consumes = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<?> addRevision(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") final String cloudId,
      @PathVariable("representationName") final String representationName,
      @PathVariable("version") final String version,
      @RequestBody Revision revision)
      throws RevisionIsNotValidException, RepresentationNotExistsException,
      AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    //
    Representation representation = Representation.fromFields(cloudId, representationName, version);
    //
    if (dataSetPermissionsVerifier.isUserAllowedToAddRevisionTo(representation)) {
      addRevisionToRepresentationVersion(revision, representation);
      return createResponseEntity(httpServletRequest, null);
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  /**
   * Adds a new revision to representation version.
   * <strong>Read permissions required.</strong>
   *
   * @param cloudId cloud id of the record (required).
   * @param representationName schema of representation (required).
   * @param version a specific version of the representation(required).
   * @param revisionName the name of revision (required).
   * @param revisionProviderId revision provider id (required).
   * @param tags set of tags (deleted)
   * @return URI to a revision tags inside a version.
   * @throws RepresentationNotExistsException representation does not exist in specified version
   * @throws RevisionIsNotValidException if the added revision was not valid
   * @statuscode 201 object has been created.
   */
  @PostMapping(value = REVISION_ADD_WITH_PROVIDER)
  public ResponseEntity<Revision> addRevision(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") final String cloudId,
      @PathVariable("representationName") final String representationName,
      @PathVariable("version") final String version,
      @PathVariable("revisionName") String revisionName,
      @PathVariable("revisionProviderId") String revisionProviderId,
      @RequestParam(value = "tags", defaultValue = "") Set<String> tags)
      throws RepresentationNotExistsException, RevisionIsNotValidException,
      AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    ParamUtil.validateTags(tags,
        new HashSet<>(Sets.newHashSet(Tags.DELETED.getTag())));
    //
    Representation representation = Representation.fromFields(cloudId, representationName, version);
    //
    if (dataSetPermissionsVerifier.isUserAllowedToAddRevisionTo(representation)) {
      Revision revision = Revision.fromParams(revisionName, revisionProviderId, tags);
      addRevisionToRepresentationVersion(revision, representation);
      return createResponseEntity(httpServletRequest, revision);
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  /**
   * Remove a revision
   *
   * @param cloudId cloud Id
   * @param representationName representation name
   * @param version representation version
   * @param revisionName revision name
   * @param revisionProviderId revision provider
   * @param revisionTimestamp revision timestamp
   * @throws RepresentationNotExistsException when the representation version doesn't exist
   * @throws AccessDeniedOrObjectDoesNotExistException when user doesn't have privileges for the dataset
   */
  @DeleteMapping(value = REVISION_DELETE)
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void deleteRevision(
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("version") String version,
      @PathVariable("revisionName") String revisionName,
      @PathVariable("revisionProviderId") String revisionProviderId,
      @RequestParam("revisionTimestamp") String revisionTimestamp)
      throws RepresentationNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    //
    Representation representation = Representation.fromFields(cloudId, representationName, version);
    //
    if (dataSetPermissionsVerifier.isUserAllowedToDeleteRevisionFor(representation)) {
      DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);
      dataSetService.deleteRevision(cloudId, representationName, version, revisionName, revisionProviderId, timestamp.toDate());
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  private void addRevisionToRepresentationVersion(Revision revision, Representation representation)
      throws RepresentationNotExistsException, RevisionIsNotValidException {

    addRevision(
        representation.getCloudId(),
        representation.getRepresentationName(),
        representation.getVersion(),
        revision);

    // insert information in extra table
    recordService.insertRepresentationRevision(
        representation.getCloudId(),
        representation.getRepresentationName(),
        revision.getRevisionProviderId(),
        revision.getRevisionName(),
        representation.getVersion(),
        revision.getCreationTimeStamp());
  }

  private void addRevision(String globalId, String schema, String version, Revision revision)
      throws RevisionIsNotValidException, RepresentationNotExistsException {

    recordService.addRevision(globalId, schema, version, revision);
    // insert information in extra table
    recordService.insertRepresentationRevision(globalId, schema, revision.getRevisionProviderId(),
        revision.getRevisionName(), version, revision.getCreationTimeStamp());

    dataSetService.updateAllRevisionDatasetsEntries(globalId, schema, version, revision);
  }

  private <T> ResponseEntity<T> createResponseEntity(HttpServletRequest httpServletRequest, T entity) {
    HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
    URI resultURI = UriComponentsBuilder.fromHttpRequest(httpRequest).build().toUri();
    ResponseEntity.BodyBuilder bb = ResponseEntity.created(resultURI);
    if (entity != null) {
      return bb.body(entity);
    } else {
      return bb.build();
    }
  }
}
