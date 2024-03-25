package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATIONS_RESOURCE;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.RepresentationsListWrapper;
import jakarta.ws.rs.core.MediaType;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resource that represents record representations.
 */
@RestController
@RequestMapping(REPRESENTATIONS_RESOURCE)
public class RepresentationsResource {

  private final RecordService recordService;

  public RepresentationsResource(RecordService recordService) {
    this.recordService = recordService;
  }

  /**
   * Returns a list of all the latest persistent versions of a record representation.
   *
   * @param cloudId cloud id of the record in which all the latest versions of representations are required.
   * @return list of representations.
   * @throws RecordNotExistsException provided id is not known to Unique Identifier Service.
   * @summary get representations
   */
  @GetMapping(produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
  public RepresentationsListWrapper getRepresentations(
      HttpServletRequest httpServletRequest,
      @PathVariable("cloudId") String cloudId) throws RecordNotExistsException {

    List<Representation> representationInfos = recordService.getRecord(cloudId).getRepresentations();
    prepare(httpServletRequest, representationInfos);
    return new RepresentationsListWrapper(representationInfos);
  }

  private void prepare(HttpServletRequest httpServletRequest, List<Representation> representationInfos) {
    for (Representation representationInfo : representationInfos) {
      representationInfo.setFiles(null);
      EnrichUriUtil.enrich(httpServletRequest, representationInfo);
    }
  }
}
