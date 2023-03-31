package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_RESOURCE;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.utils.storage_selector.StorageSelector;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.util.UriUtils;

/**
 * Resource to manage representation version's files with their content.
 */
@RestController
@RequestMapping(FILE_RESOURCE)
public class FileResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileResource.class);

  private static final String HEADER_RANGE = "Range";
  private final RecordService recordService;
  private final DataSetPermissionsVerifier dataSetPermissionsVerifier;
  private final Integer objectStoreSizeThreshold;

  public FileResource(RecordService recordService,
      Integer objectStoreSizeThreshold,
      DataSetPermissionsVerifier dataSetPermissionsVerifier) {
    this.recordService = recordService;
    this.objectStoreSizeThreshold = objectStoreSizeThreshold;
    this.dataSetPermissionsVerifier = dataSetPermissionsVerifier;
  }

  /**
   * Updates a file in a representation version. MD5 of the uploaded data is returned as a tag. Consumes multipart content - form
   * data:
   * <ul>
   * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_MIME} -
   * file mime type</li>
   * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_DATA} -
   * binary stream of file content (required).</li>
   * </ul>
   *
   * <strong>Write permissions required.</strong>
   *
   * @param cloudId cloud id of the record in which the file will be updated (required)
   * @param representationName schema of representation (required)
   * @param version a specific version of the representation(required)
   * @param mimeType mime type of file
   * @param data binary stream of file content (required)
   * @return URI of the uploaded content file in content-location
   * @throws RepresentationNotExistsException representation does not exist in specified version.
   * @throws CannotModifyPersistentRepresentationException specified representation version is persistent and modifying its files
   * is not allowed.
   * @throws FileNotExistsException specified file does not exist.
   * @summary Updates a file in a representation version
   * @statuscode 204 object has been updated.
   */
  @PutMapping
  public ResponseEntity<Void> sendFile(
      HttpServletRequest httpServletRequest,
      @PathVariable String cloudId,
      @PathVariable String representationName,
      @PathVariable String version,
      HttpServletRequest request,
      @RequestHeader(HttpHeaders.CONTENT_TYPE) String mimeType,
      InputStream data) throws RepresentationNotExistsException,
      CannotModifyPersistentRepresentationException, FileNotExistsException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Representation representation = Representation.fromFields(cloudId, representationName, version);
    if (dataSetPermissionsVerifier.isUserAllowedToUploadFileFor(representation)) {
      String fileName = extractFileNameFromURL(request);

      File f = new File();
      f.setMimeType(mimeType);
      f.setFileName(fileName);

      PreBufferedInputStream prebufferedInputStream = new PreBufferedInputStream(data, objectStoreSizeThreshold);
      f.setFileStorage(new StorageSelector(prebufferedInputStream, mimeType).selectStorage());

      // For throw  FileNotExistsException if specified file does not exist.
      recordService.getFile(cloudId, representationName, version, fileName);
      recordService.putContent(cloudId, representationName, version, f, prebufferedInputStream);
      IOUtils.closeQuietly(prebufferedInputStream);
      EnrichUriUtil.enrich(httpServletRequest, cloudId, representationName, version, f);

      ResponseEntity.BodyBuilder response = ResponseEntity
          .status(HttpStatus.NO_CONTENT)
          .location(f.getContentUri());
      if (f.getMd5() != null) {
        response.eTag(f.getMd5());
      }
      return response.build();
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  /**
   * Returns file content. Basic support for HTTP "Range" header is implemented for retrieving only a part of content .
   * (Description of Range header can be found in Hypertext Transfer Protocol HTTP/1.1, <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">section 14.35 Range</a>). For instance:
   * <ul>
   * <li><b>Range: bytes=10-15</b> - retrieve bytes from 10 to 15 of content
   * <li><b>Range: bytes=10-</b> - skip 10 first bytes of content
   * </ul>
   *
   * <strong>Read permissions required.</strong>
   *
   * @param cloudId cloud id of the record (required).
   * @param representationName schema of representation (required).
   * @param version a specific version of the representation(required).
   * @param range range of bytes to return (optional)
   * @return file content
   * @throws RepresentationNotExistsException representation does not exist in the specified version.
   * @throws RepresentationNotExistsException representation does not exist in the specified version.
   * @throws WrongContentRangeException wrong value in "Range" header
   * @throws FileNotExistsException representation version does not have file with the specified name.
   * @summary get file contents from a representation version
   */
  @GetMapping
  @PreAuthorize("isAuthenticated()")
  public ResponseEntity<StreamingResponseBody> getFile(
      @PathVariable String cloudId,
      @PathVariable String representationName,
      @PathVariable String version,
      HttpServletRequest request,
      @RequestHeader(value = HEADER_RANGE, required = false) String range)
      throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException {

    String fileName = extractFileNameFromURL(request);
    // extract range
    final ContentRange contentRange;
    if (range == null) {
      contentRange = new ContentRange(-1L, -1L);
    } else {
      contentRange = ContentRange.parse(range);
    }

    // get file md5 if complete file is requested
    String md5 = null;
    MediaType fileMimeType = null;
    HttpStatus status;
    if (contentRange.isSpecified()) {
      status = HttpStatus.PARTIAL_CONTENT;
    } else {
      status = HttpStatus.OK;
      final File requestedFile = recordService.getFile(cloudId, representationName, version, fileName);
      md5 = requestedFile.getMd5();
      if (StringUtils.isNotBlank(requestedFile.getMimeType())) {
        fileMimeType = MediaType.parseMediaType(requestedFile.getMimeType());
      }
    }

    Consumer<OutputStream> downloadMethod = recordService.getContent(cloudId, representationName, version, fileName,
        contentRange.start, contentRange.end);

    ResponseEntity.BodyBuilder response = ResponseEntity.status(status);
    if (md5 != null) {
      response.eTag(md5);
    }
    if (fileMimeType != null) {
      response.contentType(fileMimeType);
    }
    return response.body(outputStream -> downloadMethod.accept(outputStream));
  }

  /**
   * Returns only HTTP headers for file request.
   *
   * @param httpServletRequest
   * @param cloudId cloud id of the record (required).
   * @param representationName schema of representation (required).
   * @param version a specific version of the representation(required).
   * @return empty response with proper http headers
   * @throws RepresentationNotExistsException
   * @throws FileNotExistsException
   * @summary get HTTP headers for file request
   */
  @RequestMapping(method = RequestMethod.HEAD)
  @PreAuthorize("isAuthenticated()")
  public ResponseEntity<Void> getFileHeaders(
      HttpServletRequest httpServletRequest,
      @PathVariable String cloudId,
      @PathVariable final String representationName,
      @PathVariable final String version) throws RepresentationNotExistsException, FileNotExistsException {

    String fileName = extractFileNameFromURL(httpServletRequest);
    final File requestedFile = recordService.getFile(cloudId, representationName, version, fileName);
    String fileMimeType = null;
    String md5 = requestedFile.getMd5();
    if (StringUtils.isNotBlank(requestedFile.getMimeType())) {
      fileMimeType = requestedFile.getMimeType();
    }

    URI requestUri = null;
    try {
      requestUri = new URI(httpServletRequest.getRequestURI());
    } catch (URISyntaxException e) {
      LOGGER.warn("Invalid URI/URL", e);
    }

    ResponseEntity.BodyBuilder response = ResponseEntity
        .status(HttpStatus.OK)
        .location(requestUri);
    if (md5 != null) {
      response.eTag(md5);
    }
    if (fileMimeType != null) {
      response.contentType(MediaType.parseMediaType(fileMimeType));
    }
    return response.build();
  }


  /**
   * Deletes file from representation version.
   * <strong>Delete permissions required.</strong>
   *
   * @param cloudId cloud id of the record (required).
   * @param representationName schema of representation (required).
   * @param version a specific version of the representation(required).
   * @throws RepresentationNotExistsException representation does not exist in the specified version.
   * @throws FileNotExistsException representation version does not have file with the specified name.
   * @throws CannotModifyPersistentRepresentationException specified representation version is persistent and deleting its files
   * is not allowed.
   */
  @DeleteMapping
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void deleteFile(
      @PathVariable String cloudId,
      @PathVariable String representationName,
      @PathVariable String version,
      HttpServletRequest httpServletRequest) throws RepresentationNotExistsException, FileNotExistsException,
      CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException, DataSetAssignmentException {

    Representation representation = Representation.fromFields(cloudId, representationName, version);
    if (dataSetPermissionsVerifier.isUserAllowedToDeleteFileFor(representation)) {
      recordService.deleteContent(cloudId, representationName, version, extractFileNameFromURL(httpServletRequest));
    } else {
      throw new AccessDeniedOrObjectDoesNotExistException();
    }
  }

  private String extractFileNameFromURL(HttpServletRequest request) {
    return UriUtils.decode(request.getRequestURI().split("/files/")[1], StandardCharsets.UTF_8);
  }

  /**
   * Description of Range header can be found in Hypertext Transfer Protocol HTTP/1.1, <a
   * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">section 14.35 Range</a>.
   */
  static class ContentRange {

    private static final Pattern BYTES_PATTERN = Pattern.compile("bytes=(?<start>\\d+)[-](?<end>\\d*)");

    private final long start;
    private final long end;

    ContentRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    boolean isSpecified() {
      return start != -1 || end != -1;
    }

    long getStart() {
      return start;
    }

    long getEnd() {
      return end;
    }

    static ContentRange parse(String range) throws WrongContentRangeException {
      if (range == null) {
        throw new IllegalArgumentException("Range should not be null");
      }
      long start;
      long end;
      Matcher rangeMatcher = BYTES_PATTERN.matcher(range);
      if (rangeMatcher.matches()) {
        try {
          start = Long.parseLong(rangeMatcher.group("start"));
          String endString = rangeMatcher.group("end");
          end = endString.isEmpty() ? -1 : Long.parseLong(endString);
        } catch (NumberFormatException ex) {
          throw new WrongContentRangeException("Cannot parse range: " + ex.getMessage());
        }
      } else {
        throw new WrongContentRangeException("Expected range header format is: " + BYTES_PATTERN.pattern());
      }

      if (end != -1 && end < start) {
        throw new WrongContentRangeException("Range end must not be smaller than range start");
      }

      return new ContentRange(start, end);
    }
  }
}
