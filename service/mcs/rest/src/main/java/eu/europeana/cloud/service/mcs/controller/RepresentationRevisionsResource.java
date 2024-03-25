package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_REVISIONS_RESOURCE;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.RepresentationsListWrapper;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource to manage representations.
 */
@RestController
@RequestMapping(REPRESENTATION_REVISIONS_RESOURCE)
public class RepresentationRevisionsResource {


  private final RecordService recordService;

  public RepresentationRevisionsResource(RecordService recordService) {
    this.recordService = recordService;
  }

  /**
   * Returns the representation version which associates cloud identifier, representation name with revision identifier, provider
   * and timestamp.
   *
   * @param cloudId cloud id of the record which contains the representation .
   * @param representationName name of the representation .
   * @param revisionName name of the revision associated with this representation version
   * @param revisionProviderId identifier of institution that provided the revision
   * @param revisionTimestamp timestamp of the specific revision, if not given the latest revision with revisionName created by
   * revisionProviderId will be considered (timestamp should be given in UTC format)
   * @return requested specific representation object.
   * @throws RepresentationNotExistsException when representation doesn't exist
   * @summary get a representation response object
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public RepresentationsListWrapper getRepresentationRevisions(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("revisionName") String revisionName,
      @RequestParam("revisionProviderId") String revisionProviderId,
      @RequestParam(value="revisionTimestamp", required = false) String revisionTimestamp) throws RepresentationNotExistsException {

    Date revisionDate = null;
    if (revisionTimestamp != null) {
      DateTime utc = new DateTime(revisionTimestamp, DateTimeZone.UTC);
      revisionDate = utc.toDate();
    }
    List<RepresentationRevisionResponse> info =
        recordService.getRepresentationRevisions(cloudId, representationName, revisionProviderId, revisionName, revisionDate);
    List<Representation> representations = new ArrayList<>();
    if (info != null) {
      for (RepresentationRevisionResponse representationRevisionsResource : info) {
        Representation representation;
        representation = recordService.getRepresentation(
            representationRevisionsResource.getCloudId(),
            representationRevisionsResource.getRepresentationName(),
            representationRevisionsResource.getVersion());
        EnrichUriUtil.enrich(httpServletRequest, representation);
        representations.add(representation);
      }
    } else {
      throw new RepresentationNotExistsException("No representation was found");
    }

    return new RepresentationsListWrapper(representations);
  }
}
