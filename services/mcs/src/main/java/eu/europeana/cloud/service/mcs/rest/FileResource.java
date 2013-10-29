package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.ContentService;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

/**
 * FilesResource
 */
@Path("/records/{ID}/representations/{REPRESENTATION}/versions/{VERSION}/files/{FILE}")
@Component
public class FileResource {

    @Autowired
    private RecordService recordService;

    @Autowired
    private ContentService contentService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_REP)
    private String representation;

    @PathParam(P_VER)
    private String version;

    @PathParam(P_FILE)
    private String fileName;

    static final String HEADER_RANGE = "Range";


    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(
            @FormDataParam(F_FILE_MIME) String mimeType,
            @FormDataParam(F_FILE_DATA) InputStream data)
            throws IOException, RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        ParamUtil.require(F_FILE_DATA, data);

        File f = new File();
        f.setMimeType(mimeType);
        f.setFileName(fileName);

        boolean isUpdateOperation = false;
        Representation rep = null;
        try {
            rep = recordService.addFileToRepresentation(globalId, representation, version, f);
        } catch (FileAlreadyExistsException e) {
            isUpdateOperation = true;
            rep = recordService.getRepresentation(globalId, representation, version);
        }
        contentService.putContent(rep, f, data);
        EnrichUriUtil.enrich(uriInfo, rep, f);

        Response.Status operationStatus = isUpdateOperation ? Response.Status.NO_CONTENT : Response.Status.CREATED;
        return Response.status(operationStatus).location(f.getContentUri()).tag(f.getMd5()).build();
    }


    @GET
    public Response getFile(
            @HeaderParam(HEADER_RANGE) String range)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, FileNotExistsException {
        // extract range
        final ContentRange contentRange = ContentRange.parse(range);

        final Representation rep = recordService.getRepresentation(globalId, representation, version);
        final File requestedFile = getByName(fileName, rep);

        if (requestedFile == null) {
            throw new FileNotExistsException();
        }

        StreamingOutput output = new StreamingOutput() {

            @Override
            public void write(OutputStream output)
                    throws IOException, WebApplicationException {
                try {
                    contentService.getContent(rep, requestedFile, contentRange.start, contentRange.end, output);
                } catch (FileNotExistsException ex) {
                    throw new WebApplicationException(ex);
                }
            }
        };
        return Response.ok(output).tag(requestedFile.getMd5()).build();
    }


    @DELETE
    public Response deleteFile() {
        contentService.deleteContent(globalId, representation, version, fileName);
        return Response.noContent().build();
    }


    private File getByName(String fileName, Representation rep) {
        for (File f : rep.getFiles()) {
            if (f.getFileName().equals(fileName)) {
                return f;
            }
        }
        return null;
    }

    private static class ContentRange {

        long start, end;


        ContentRange(long start, long end) {
            this.start = start;
            this.end = end;
        }


        static ContentRange parse(String range) {
            long start = -1,
                    end = -1;
            if (range != null) {
                if (range.startsWith("bytes=")) {
                    range = range.substring("bytes=".length());
                    int minus = range.indexOf('-');
                    try {
                        if (minus > 0) {
                            start = Long.parseLong(range.substring(0, minus));
                            end = Long.parseLong(range.substring(minus + 1));
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
            return new ContentRange(start, end);
        }
    }
}
