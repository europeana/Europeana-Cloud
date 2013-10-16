package eu.europeana.cloud.contentserviceapi.rest;

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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.FileAlreadyExistsException;
import eu.europeana.cloud.contentserviceapi.service.ContentService;
import eu.europeana.cloud.contentserviceapi.service.RecordService;
import eu.europeana.cloud.definitions.model.File;
import eu.europeana.cloud.definitions.model.Representation;
import static eu.europeana.cloud.contentserviceapi.rest.PathConstants.*;

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
            throws FileAlreadyExistsException, IOException {
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
    public StreamingOutput getFile(
            @HeaderParam(HEADER_RANGE) String range) {
        // extract range
        final ContentRange contentRange = ContentRange.parse(range);

        final Representation rep = recordService.getRepresentation(globalId, representation, version);
        final File f = new File();
        f.setFileName(fileName);
        return new StreamingOutput() {

            @Override
            public void write(OutputStream output)
                    throws IOException, WebApplicationException {
                contentService.writeContent(rep, f, contentRange.start, contentRange.end, output);
            }
        };
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
