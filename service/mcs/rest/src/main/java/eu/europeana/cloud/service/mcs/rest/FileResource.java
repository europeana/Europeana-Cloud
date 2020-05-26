package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.utils.storage_selector.StorageSelector;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.util.MimeType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HeaderParam;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.nullToEmpty;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_RESOURCE;
import static eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream.wrap;

/**
 * Resource to manage representation version's files with their content.
 */
@RestController
@RequestMapping(FILE_RESOURCE)
@Scope("request")
public class FileResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileResource.class);

    private static final String HEADER_RANGE = "Range";

    @Autowired
    private RecordService recordService;
	@Autowired
	private MutableAclService mutableAclService;
    @Autowired
    private Integer objectStoreSizeThreshold;

    /**
     *  Updates a file in a representation version. MD5 of
     * the uploaded data is returned as a tag. Consumes multipart content - form data:
     * <ul>
     * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_MIME} -
     * file mime type</li>
     * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_DATA} -
     * binary stream of file content (required).</li>
     * </ul>
     *
     *<strong>Write permissions required.</strong>
     *@summary Updates a file in a representation version
     * @param cloudId cloud id of the record in which the file will be updated (required)
     * @param representationName schema of representation (required)
     * @param version a specific version of the representation(required)
     * @param fileName the name of the file(required)
     * @param mimeType mime type of file
     * @param data binary stream of file content (required)
     * @return URI of the uploaded content file in content-location
     * @throws RepresentationNotExistsException representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException specified representation version is persistent and modifying
     *                                                       its files is not allowed.
     * @throws FileNotExistsException specified file does not exist.
     * @statuscode 204 object has been updated.
     */
    @PutMapping
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', write)")
    public ResponseEntity<?> sendFile(
            HttpServletRequest httpServletRequest,
    		@PathVariable String cloudId,
    		@PathVariable String representationName,
    		@PathVariable String version,
    		@PathVariable String fileName,
    		@RequestHeader(HttpHeaders.CONTENT_TYPE) String mimeType,
            InputStream data) throws RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, FileNotExistsException, IOException {

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

        return ResponseEntity
                .status(HttpStatus.NO_CONTENT)
                .location(f.getContentUri())
                .eTag(nullToEmpty(f.getMd5()))
                .build();
    }

    /**
     * Returns file content. Basic support for HTTP "Range" header is
     * implemented for retrieving only a part of content .
     * (Description of Range header can be found in Hypertext Transfer Protocol
     * HTTP/1.1, <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">section
     * 14.35 Range</a>). For instance:
     * <ul>
     * <li><b>Range: bytes=10-15</b> - retrieve bytes from 10 to 15 of content
     * <li><b>Range: bytes=10-</b> - skip 10 first bytes of content
     * </ul>
     *
     * <strong>Read permissions required.</strong>
     * @summary get file contents from a representation version
     * @param cloudId cloud id of the record (required).
     * @param representationName schema of representation (required).
     * @param version a specific version of the representation(required).
     * @param fileName the name of the file(required).
     * @param range range of bytes to return (optional)
     * @return file content
     * @throws RepresentationNotExistsException
     * representation does not exist in the specified version.
     * @throws RepresentationNotExistsException representation does not exist in
     * the specified version.
     * @throws WrongContentRangeException wrong value in "Range" header
     * @throws FileNotExistsException representation version does not have file
     * with the specified name.
     */
    @GetMapping
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<StreamingResponseBody> getFile(
            @PathVariable String cloudId,
            @PathVariable String representationName,
    		@PathVariable String version,
    		@PathVariable final String fileName,
            @RequestHeader(value = HEADER_RANGE,required = false) String range)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException {

        // extract range
        final ContentRange contentRange;
        if (range == null) {
            contentRange = new ContentRange(-1L, -1L);
        } else {
            contentRange = ContentRange.parse(range);
        }

        // get file md5 if complete file is requested
        String md5 = null;
        MediaType fileMimeType = MediaType.APPLICATION_OCTET_STREAM;
        HttpStatus status;
        if (contentRange.isSpecified()) {
            status = HttpStatus.PARTIAL_CONTENT;
        } else {
            status = HttpStatus.OK;
            final File requestedFile = recordService.getFile(cloudId, representationName, version, fileName);
            md5 = requestedFile.getMd5();
            if(StringUtils.isNotBlank(requestedFile.getMimeType())){
                fileMimeType = MediaType.parseMediaType(requestedFile.getMimeType());
            }
        }

        Consumer<OutputStream> downloadMethod = recordService.getContent(cloudId, representationName, version, fileName,
                contentRange.start, contentRange.end);

        return ResponseEntity
                .status(status)
                .contentType(fileMimeType)
                .eTag(nullToEmpty(md5))
                .body(outputStream -> downloadMethod.accept(outputStream));
    }

    /**
     *
     * Returns only HTTP headers for file request.
     *
     * @param httpServletRequest
     * @param cloudId cloud id of the record (required).
     * @param representationName schema of representation (required).
     * @param version a specific version of the representation(required).
     * @param fileName the name of the file(required).
     *
     * @return empty response with proper http headers
     * @summary get HTTP headers for file request
     * @throws RepresentationNotExistsException
     * @throws FileNotExistsException
     */
    @RequestMapping(method = RequestMethod.HEAD)
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<?> getFileHeaders(
            HttpServletRequest httpServletRequest,
            @PathVariable String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @PathVariable final String fileName) throws RepresentationNotExistsException, FileNotExistsException {

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

        return ResponseEntity
                .status(HttpStatus.OK)
                .contentType(MediaType.parseMediaType(fileMimeType))
                .location(requestUri)
                .eTag(nullToEmpty(md5))
                .build();
    }


    /**
     * Deletes file from representation version.
     *<strong>Delete permissions required.</strong>
     *
     * @param cloudId cloud id of the record (required).
     * @param representationName schema of representation (required).
     * @param version a specific version of the representation(required).
     * @param fileName the name of the file(required).
     *
     * @throws RepresentationNotExistsException representation does not exist in
     * the specified version.
     * @throws FileNotExistsException representation version does not have file
     * with the specified name.
     * @throws CannotModifyPersistentRepresentationException specified
     * representation version is persistent and deleting its files is not
     * allowed.
     */
    @DeleteMapping
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', delete)")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteFile(
            @PathVariable String cloudId,
            @PathVariable String representationName,
    		@PathVariable String version,
    		@PathVariable String fileName) throws RepresentationNotExistsException, FileNotExistsException,
                                                                    CannotModifyPersistentRepresentationException {

        recordService.deleteContent(cloudId, representationName, version, fileName);
    }

    /**
     * Description of Range header can be found in Hypertext Transfer Protocol
     * HTTP/1.1, <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">section
     * 14.35 Range</a>.
     */
    static class ContentRange {
        private static final Pattern BYTES_PATTERN = Pattern.compile("bytes=(?<start>\\d+)[-](?<end>\\d*)");

        private long start;
        private long end;

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

            long start;
            long end;
            if (range == null) {
                throw new IllegalArgumentException("Range should not be null");
            }
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
