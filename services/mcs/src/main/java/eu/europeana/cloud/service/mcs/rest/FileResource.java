package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
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
import eu.europeana.cloud.service.mcs.service.ContentService;
import eu.europeana.cloud.service.mcs.service.RecordService;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

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


    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(
            @FormDataParam("mimeType") String mimeType,
            @FormDataParam("data") InputStream data)
            throws FileAlreadyExistsException, IOException, RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        if (data == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity("You must provide data.").build();
        }
        Representation rep = recordService.getRepresentation(globalId, representation, version);
        File f = new File();
        f.setMimeType(mimeType);
        f.setFileName(fileName);
        contentService.insertContent(rep, f, data);
        URI fileUri = uriInfo.getAbsolutePathBuilder().path(f.getFileName()).build();
        return Response.created(fileUri).build();
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
                    contentService.writeContent(rep, requestedFile, contentRange.start, contentRange.end, output);
                } catch (FileNotExistsException ex) {
                    throw new WebApplicationException(ex);
                }
            }
        };
        return Response.ok(output).build();
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
