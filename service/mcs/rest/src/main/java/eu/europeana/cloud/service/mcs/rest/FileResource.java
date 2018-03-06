package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.UnitedExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.storage.selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.rest.storage.selector.StorageSelector;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.rest.storage.selector.PreBufferedInputStream.wrap;

/**
 * Resource to manage representation version's files with their content.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/files/{"
        + P_FILENAME + ":(.+)?}")
@Component
@Scope("request")
public class FileResource {
    private static final String HEADER_RANGE = "Range";

    @Autowired
    private RecordService recordService;
	@Autowired
	private MutableAclService mutableAclService;
    @Autowired
    private Integer objectStoreSizeThreshold;

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

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
     * @param globalId cloud id of the record in which the file will be updated (required)
     * @param schema schema of representation (required)
     * @param version a specific version of the representation(required)
     * @param fileName the name of the file(required)
     * @param mimeType mime type of file
     * @param data binary stream of file content (required)
     * @return URI of the uploaded content file in content-location
     * @throws RepresentationNotExistsException representation does not exist in
     * specified version.
     * @throws CannotModifyPersistentRepresentationException specified
     * representation version is persistent and modifying its files is not
     * allowed.
     * @throws FileNotExistsException specified file does not exist.
     * @statuscode 204 object has been updated.
     */
    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', write)")
    public Response sendFile(@Context UriInfo uriInfo,
    		@PathParam(P_CLOUDID) String globalId,
    		@PathParam(P_REPRESENTATIONNAME) String schema,
    		@PathParam(P_VER) String version,
    		@PathParam(P_FILENAME) String fileName,
    		@FormDataParam(F_FILE_MIME) String mimeType, @FormDataParam(F_FILE_DATA) InputStream data)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            FileNotExistsException {
        ParamUtil.require(F_FILE_DATA, data);
        ParamUtil.require(F_FILE_MIME, mimeType);
        File f = new File();
        f.setMimeType(mimeType);
        f.setFileName(fileName);

        PreBufferedInputStream prebufferedInputStream = wrap(data, objectStoreSizeThreshold);
        f.setFileStorage(new StorageSelector(prebufferedInputStream, mimeType).selectStorage());

        // For throw  FileNotExistsException if specified file does not exist.
        recordService.getFile(globalId, schema, version, fileName);
        recordService.putContent(globalId, schema, version, f, prebufferedInputStream);
        IOUtils.closeQuietly(prebufferedInputStream);
        EnrichUriUtil.enrich(uriInfo, globalId, schema, version, f);

        return Response.status(Response.Status.NO_CONTENT).location(f.getContentUri()).tag(f.getMd5()).build();
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
     * @param globalId cloud id of the record (required).
     * @param schema schema of representation (required).
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
    @GET
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', read)")
    public Response getFile(@PathParam(P_CLOUDID) final String globalId, 
    		@PathParam(P_REPRESENTATIONNAME) final String schema,
    		@PathParam(P_VER) final String version,
    		@PathParam(P_FILENAME) final String fileName,
    		@HeaderParam(HEADER_RANGE) String range)
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
        String fileMimeType = null;
        Response.Status status;
        if (contentRange.isSpecified()) {
            status = Response.Status.PARTIAL_CONTENT;
        } else {
            status = Response.Status.OK;
            final File requestedFile = recordService.getFile(globalId, schema, version, fileName);
            md5 = requestedFile.getMd5();
            if(StringUtils.isNotBlank(requestedFile.getMimeType())){
                fileMimeType = requestedFile.getMimeType();
            }
        }

        // stream output
        StreamingOutput output = new StreamingOutput() {
            @Override
            public void write(OutputStream output)
                    throws IOException, WebApplicationException {
                try {
                    recordService.getContent(globalId, schema, version, fileName, contentRange.start, contentRange.end,
                            output);
                } catch (RepresentationNotExistsException ex) {
                    throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                } catch (FileNotExistsException ex) {
                    throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                } catch (WrongContentRangeException ex) {
                    throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                }
            }
        };

        return Response.status(status).entity(output).type(fileMimeType).tag(md5).build();
    }

    /**
     * 
     * Returns only HTTP headers for file request. 
     * 
     * @param uriInfo
     * @param globalId cloud id of the record (required).
     * @param schema schema of representation (required).
     * @param version a specific version of the representation(required).
     * @param fileName the name of the file(required).
     *              
     * @return empty response with proper http headers
     * @summary get HTTP headers for file request
     * @throws RepresentationNotExistsException
     * @throws FileNotExistsException
     */
    @HEAD
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public Response getFileHeaders(@Context UriInfo uriInfo,
                                   @PathParam(P_CLOUDID) final String globalId,
                                   @PathParam(P_REPRESENTATIONNAME) final String schema,
                                   @PathParam(P_VER) final String version,
                                   @PathParam(P_FILENAME) final String fileName)
            throws RepresentationNotExistsException, FileNotExistsException {

        URI requestUri = uriInfo.getRequestUri();
        final File requestedFile = recordService.getFile(globalId, schema, version, fileName);
        String fileMimeType = null;
        String md5 = requestedFile.getMd5();
        if (StringUtils.isNotBlank(requestedFile.getMimeType())) {
            fileMimeType = requestedFile.getMimeType();
        }

        return Response.status(Response.Status.OK).type(fileMimeType).location(requestUri).tag(md5).build();
    }
    
    
    /**
     * Deletes file from representation version.
     *<strong>Delete permissions required.</strong>
     *
     * @param globalId cloud id of the record (required).
     * @param schema schema of representation (required).
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
    @DELETE
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
    		+ " 'eu.europeana.cloud.common.model.Representation', delete)")
    public void deleteFile(@PathParam(P_CLOUDID) final String globalId, @PathParam(P_REPRESENTATIONNAME) String schema, 
    		@PathParam(P_VER) String version,
    		@PathParam(P_FILENAME) String fileName)
            throws RepresentationNotExistsException, FileNotExistsException,
            CannotModifyPersistentRepresentationException {
    	
        recordService.deleteContent(globalId, schema, version, fileName);
    }

    /**
     * Description of Range header can be found in Hypertext Transfer Protocol
     * HTTP/1.1, <a
     * href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35">section
     * 14.35 Range</a>.
     */
    static class ContentRange {

        private long start, end;

        private static final Pattern BYTES_PATTERN = Pattern.compile("bytes=(?<start>\\d+)[-](?<end>\\d*)");

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

        static ContentRange parse(String range)
                throws WrongContentRangeException {
            long start, end;
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
