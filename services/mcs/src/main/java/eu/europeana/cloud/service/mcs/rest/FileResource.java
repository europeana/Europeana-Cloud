package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

/**
 * FilesResource
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_REP + "}/versions/{" + P_VER + "}/files/{" + P_FILE + "}")
@Component
public class FileResource {

    @Autowired
    private RecordService recordService;

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

        boolean isCreateOperation = recordService.putContent(globalId, representation, version, f, data);
        EnrichUriUtil.enrich(uriInfo, globalId, representation, version, f);

        Response.Status operationStatus = isCreateOperation ? Response.Status.CREATED : Response.Status.NO_CONTENT;
        return Response.status(operationStatus).location(f.getContentUri()).tag(f.getMd5()).build();
    }


    @GET
    public Response getFile(
            @HeaderParam(HEADER_RANGE) String range)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, FileNotExistsException {
        // extract range
        final ContentRange contentRange = ContentRange.parse(range);

        // TODO: this is some kind of logic here, we do not want this
        String md5 = null;
        if (!contentRange.isSpecified()) {
            final Representation rep = recordService.getRepresentation(globalId, representation, version);
            final File requestedFile = getByName(fileName, rep);

            if (requestedFile == null) {
                throw new FileNotExistsException();
            }
            md5 = requestedFile.getMd5();
        }

        StreamingOutput output = new StreamingOutput() {

            @Override
            public void write(OutputStream output)
                    throws IOException, WebApplicationException {
                recordService.getContent(globalId, representation, version, fileName, contentRange.start, contentRange.end, output);
            }
        };

        return Response.ok(output).tag(md5).build();
//        return Response.ok(output).build();
    }


    private File getByName(String fileName, Representation rep) {
        for (File f : rep.getFiles()) {
            if (f.getFileName().equals(fileName)) {
                return f;
            }
        }
        return null;
    }


    @DELETE
    public Response deleteFile() {
        recordService.deleteContent(globalId, representation, version, fileName);
        return Response.noContent().build();
    }

    private static class ContentRange {

        long start, end;


        ContentRange(long start, long end) {
            this.start = start;
            this.end = end;
        }


        boolean isSpecified() {
            return start != -1 || end != -1;
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
