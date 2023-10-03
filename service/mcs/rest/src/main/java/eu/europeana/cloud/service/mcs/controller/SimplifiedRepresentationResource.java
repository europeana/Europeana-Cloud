package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.SIMPLIFIED_REPRESENTATION_RESOURCE;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Gives access to latest persistent representation using 'friendly' URL
 */
@RestController
@RequestMapping(SIMPLIFIED_REPRESENTATION_RESOURCE)
public class SimplifiedRepresentationResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimplifiedRepresentationResource.class);

  private final UISClientHandler uisClientHandler;
  private final RecordService recordService;

  public SimplifiedRepresentationResource(UISClientHandler uisClientHandler, RecordService recordService) {
    this.uisClientHandler = uisClientHandler;
    this.recordService = recordService;
  }

  /**
   * Returns the latest persistent version of a given representation.
   *
   * @param httpServletRequest
   * @param providerId
   * @param localId
   * @param representationName
   * @return
   * @throws CloudException
   * @throws RepresentationNotExistsException
   * @summary Get representation using simplified url
   */
  @GetMapping
  @PreAuthorize("isAuthenticated()")
  public @ResponseBody Representation getRepresentation(
      HttpServletRequest httpServletRequest,
      @PathVariable String providerId,
      @PathVariable String localId,
      @PathVariable String representationName) throws RepresentationNotExistsException,
      ProviderNotExistsException, RecordNotExistsException {

    LOGGER.info("Reading representation '{}' using 'friendly' approach for providerId: {} and localId: {}", representationName,
        providerId, localId);
    final String cloudId = findCloudIdFor(providerId, localId);

    Representation representation = recordService.getRepresentation(cloudId, representationName);
    EnrichUriUtil.enrich(httpServletRequest, representation);

    return representation;
  }

  private String findCloudIdFor(String providerID, String localId) throws ProviderNotExistsException, RecordNotExistsException {
    CloudId foundCloudId = uisClientHandler.getCloudIdFromProviderAndLocalId(providerID, localId);
    return foundCloudId.getId();
  }
}
