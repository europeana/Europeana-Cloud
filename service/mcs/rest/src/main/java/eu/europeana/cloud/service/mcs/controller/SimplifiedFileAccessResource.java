package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.SIMPLIFIED_FILE_ACCESS_RESOURCE;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.selectors.LatestPersistentRepresentationVersionSelector;
import eu.europeana.cloud.common.selectors.RepresentationSelector;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.util.UriUtils;

/**
 * Gives (read) access to files stored in ecloud in simplified (friendly) way. <br/> The latest persistent version of
 * representation is picked up.
 */
@RestController
@RequestMapping(SIMPLIFIED_FILE_ACCESS_RESOURCE)
public class SimplifiedFileAccessResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimplifiedFileAccessResource.class);

  private final RecordService recordService;
  private final UISClientHandler uisClientHandler;

  public SimplifiedFileAccessResource(RecordService recordService, UISClientHandler uisClientHandler) {
    this.recordService = recordService;
    this.uisClientHandler = uisClientHandler;
  }

  /**
   * Returns file content from <b>latest persistent version</b> of specified representation.
   *
   * @param providerId providerId
   * @param localId localId
   * @param representationName representationName
   * @return Requested file context
   * FileName is not treated as param since it can contain "/" that are not detectable by spring,
   * so it's value is taken directly from request url
   * @throws RepresentationNotExistsException
   * @throws FileNotExistsException
   * @throws CloudException
   * @throws RecordNotExistsException
   * @summary Get file content using simplified url
   * @statuscode 204 object has been updated.
   */
  @GetMapping
  public ResponseEntity<StreamingResponseBody> getFile(
      HttpServletRequest httpServletRequest,
      @PathVariable("providerId") final String providerId,
      @PathVariable("localId") final String localId,
      @PathVariable("representationName") final String representationName) throws RepresentationNotExistsException,
      FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, WrongContentRangeException {
    String fileName = extractFileNameFromURL(httpServletRequest, representationName);

    LOGGER.info("Reading file in friendly way for: provider: {}, localId: {}, represenatation: {}, fileName: {}",
        providerId, localId, representationName, fileName);

    final String cloudId = findCloudIdFor(providerId, localId);
    final Representation representation = selectRepresentationVersion(cloudId, representationName);
    if (representation == null) {
      throw new RepresentationNotExistsException();
    }

    final File requestedFile = readFile(cloudId, representationName, representation.getVersion(), fileName);

    String md5 = requestedFile.getMd5();
    MediaType fileMimeType = null;
    if (StringUtils.isNotBlank(requestedFile.getMimeType())) {
      fileMimeType = MediaType.parseMediaType(requestedFile.getMimeType());
    }
    EnrichUriUtil.enrich(httpServletRequest, representation, requestedFile);
    final FileResource.ContentRange contentRange = new FileResource.ContentRange(-1L, -1L);
    Consumer<OutputStream> downloadingMethod = recordService.getContent(cloudId, representationName, representation.getVersion(),
        fileName, contentRange.getStart(), contentRange.getEnd());

    ResponseEntity.BodyBuilder response = ResponseEntity
        .status(HttpStatus.OK)
        .location(requestedFile.getContentUri());
    if (md5 != null) {
      response.eTag(md5);
    }
    if (fileMimeType != null) {
      response.contentType(fileMimeType);
    }
    return response.body(output -> downloadingMethod.accept(output));
  }

  private static String extractFileNameFromURL(HttpServletRequest httpServletRequest, String representationName) {
    return UriUtils.decode(httpServletRequest.getRequestURI()
            .substring(httpServletRequest.getRequestURI().indexOf(representationName) + representationName.length() + 1), StandardCharsets.UTF_8);
  }

  /**
   * Returns file headers from <b>latest persistent version</b> of specified representation.
   *
   * @param httpServletRequest
   * @param providerId providerId
   * @param localId localId
   * @param representationName representationName
   * FileName is not treated as param since it can contain "/" that are not detectable by spring,
   * so it's value is taken directly from request url
   * @return Requested file headers (together with full file path in 'Location' header)
   * @throws RepresentationNotExistsException
   * @throws FileNotExistsException
   * @throws CloudException
   * @throws RecordNotExistsException
   * @throws ProviderNotExistsException
   * @summary Get file headers using simplified url
   */
  @RequestMapping(method = RequestMethod.HEAD)
  public ResponseEntity<?> getFileHeaders(
      HttpServletRequest httpServletRequest,
      @PathVariable("providerId") final String providerId,
      @PathVariable("localId") final String localId,
      @PathVariable("representationName") final String representationName) throws RepresentationNotExistsException,
      FileNotExistsException, RecordNotExistsException, ProviderNotExistsException {
    String fileName = extractFileNameFromURL(httpServletRequest, representationName);

    LOGGER.info("Reading file headers in friendly way for: provider: {}, localId: {}, represenatation: {}, fileName: {}",
        providerId, localId, representationName, fileName);

    final String cloudId = findCloudIdFor(providerId, localId);
    final Representation representation = selectRepresentationVersion(cloudId, representationName);
    if (representation == null) {
      throw new RepresentationNotExistsException();
    }

    final File requestedFile = readFile(cloudId, representationName, representation.getVersion(), fileName);

    String md5 = requestedFile.getMd5();
    MediaType fileMimeType = null;
    if (StringUtils.isNotBlank(requestedFile.getMimeType())) {
      fileMimeType = MediaType.parseMediaType(requestedFile.getMimeType());
    }
    EnrichUriUtil.enrich(httpServletRequest, representation, requestedFile);

    ResponseEntity.BodyBuilder response = ResponseEntity
        .status(HttpStatus.OK)
        .location(requestedFile.getContentUri());
    if (md5 != null) {
      response.eTag(md5);
    }
    if (fileMimeType != null) {
      response.contentType(fileMimeType);
    }
    return response.build();
  }

  private String findCloudIdFor(String providerID, String localId) throws ProviderNotExistsException, RecordNotExistsException {
    CloudId foundCloudId = uisClientHandler.getCloudIdFromProviderAndLocalId(providerID, localId);
    return foundCloudId.getId();
  }

  private Representation selectRepresentationVersion(String cloudId, String representationName)
      throws RepresentationNotExistsException {
    List<Representation> representations = recordService.listRepresentationVersions(cloudId, representationName);
    RepresentationSelector representationSelector = new LatestPersistentRepresentationVersionSelector();
    return representationSelector.select(representations);
  }

  private File readFile(String cloudId, String representationName, String version, String fileName)
      throws RepresentationNotExistsException, FileNotExistsException {

    return recordService.getFile(cloudId, representationName, version, fileName);
  }

}
