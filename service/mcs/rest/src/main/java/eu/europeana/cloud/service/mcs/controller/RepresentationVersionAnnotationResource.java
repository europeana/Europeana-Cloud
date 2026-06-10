package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.dto.AnnotationsDto;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION_ANNOTATION_RESOURCE;

/**
 * Resource responsible for manipulating {@link RepresentationVersionAnnotation}s for {@link Representation}s
 */
@RestController
public class RepresentationVersionAnnotationResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationVersionAnnotationResource.class.getName());

  private final DataSetPermissionsVerifier dataSetPermissionsVerifier;
  private final RecordService recordService;

  /**
   * Constructor for {@link RepresentationVersionAnnotationResource}
   *
   * @param dataSetPermissionsVerifier {@link DataSetPermissionsVerifier}
   * @param recordService {@link RecordService}
   */
  public RepresentationVersionAnnotationResource(DataSetPermissionsVerifier dataSetPermissionsVerifier, RecordService recordService) {
    this.dataSetPermissionsVerifier = dataSetPermissionsVerifier;
    this.recordService = recordService;
  }

  /**
   * Adds {@link RepresentationVersionAnnotation} to {@link Representation}
   *
   * @param cloudId {@link eu.europeana.cloud.common.model.CloudId} of the {@link Representation}
   * @param representationName representationName of the {@link Representation}
   * @param version representationVersion of the {@link Representation}
   * @param annotationsDto {@link AnnotationsDto} containing all the information about annotations that should be added to the {@link Representation}
   *
   * @return status of the operation
   * @throws AccessDeniedOrObjectDoesNotExistException in case of lack of permissions
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  @PostMapping(value = REPRESENTATION_VERSION_ANNOTATION_RESOURCE, consumes = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Void> addAnnotationToRepresentationVersion(
          @PathVariable final String cloudId,
          @PathVariable final String representationName,
          @PathVariable final String version,
          @RequestBody() AnnotationsDto annotationsDto
  ) throws AccessDeniedOrObjectDoesNotExistException, RepresentationNotExistsException {

    Representation representation = Representation.fromFields(cloudId, representationName, version);
    if (dataSetPermissionsVerifier.isUserAllowedToAddAnnotationTo(representation)) {
      LOGGER.debug("Adding annotation to representation version");
      addAnnotationsToRepresentationVersion(annotationsDto.getAnnotation(), representation);
    }else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }

    return ResponseEntity.ok().build();
  }

  private void addAnnotationsToRepresentationVersion(RepresentationVersionAnnotation annotation, Representation representation) throws RepresentationNotExistsException {
    recordService.addAnnotationToRepresentationVersion(representation, annotation);
  }

}
