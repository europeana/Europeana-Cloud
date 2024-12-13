package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_RAW_REVISIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_REVISIONS_RESOURCE;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.RepresentationRevisionResponseListWrapper;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource to manage representations.
 */
@RestController
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
  @GetMapping(path = REPRESENTATION_REVISIONS_RESOURCE, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public RepresentationsListWrapper getRepresentationRevisions(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("revisionName") String revisionName,
      @RequestParam("revisionProviderId") String revisionProviderId,
      @RequestParam(value="revisionTimestamp", required = false) String revisionTimestamp) throws RepresentationNotExistsException {

    Date revisionDate = parseRevisionTimestamp(revisionTimestamp);
    List<RepresentationRevisionResponse> info =
        recordService.getRepresentationRevisions(cloudId, representationName, revisionProviderId, revisionName, revisionDate);

    List<Representation> representations = new ArrayList<>();
    for (RepresentationRevisionResponse representationRevisionsResource : info) {
      Representation representation;
      representation = recordService.getRepresentation(
          representationRevisionsResource.getCloudId(),
          representationRevisionsResource.getRepresentationName(),
          representationRevisionsResource.getVersion());
      EnrichUriUtil.enrich(httpServletRequest, representation);
      representations.add(representation);
    }

    return new RepresentationsListWrapper(representations);
  }

  /**
   * Returns the raw representation version which associates cloud identifier, representation name with revision identifier, provider
   * and timestamp.
   *
   * @param httpServletRequest the http request
   * @param cloudId cloud id of the record which contains the representation .
   * @param representationName name of the representation .
   * @param revisionName name of the revision associated with this representation version
   * @param revisionProviderId identifier of institution that provided the revision
   * @param revisionTimestamp timestamp of the specific revision, if not given the latest revision with revisionName created by
   * revisionProviderId will be considered (timestamp should be given in UTC format)
   * @return requested specific representation object.
   * @summary get a representation versions response object
   */
  @GetMapping(path = REPRESENTATION_RAW_REVISIONS_RESOURCE, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
  public RepresentationRevisionResponseListWrapper getRepresentationRawRevisions(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId,
      @PathVariable("representationName") String representationName,
      @PathVariable("revisionName") String revisionName,
      @RequestParam("revisionProviderId") String revisionProviderId,
      @RequestParam(value = "revisionTimestamp", required = false) String revisionTimestamp) {

    List<RepresentationRevisionResponse> result = recordService.getRepresentationRevisions(cloudId,
        representationName, revisionProviderId, revisionName, parseRevisionTimestamp(revisionTimestamp));

    for (RepresentationRevisionResponse response : result) {
      EnrichUriUtil.enrich(httpServletRequest, response);
    }
    return new RepresentationRevisionResponseListWrapper(result);
  }

  private static Date parseRevisionTimestamp(String revisionTimestamp) {
    Date revisionDate = null;
    if (revisionTimestamp != null) {
      DateTime utc = new DateTime(revisionTimestamp, DateTimeZone.UTC);
      revisionDate = utc.toDate();
    }
    return revisionDate;
  }
}
