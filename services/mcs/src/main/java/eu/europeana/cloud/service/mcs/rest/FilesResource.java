package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.service.ContentService;
import eu.europeana.cloud.service.mcs.service.RecordService;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

/**
 * FilesResource
 */
@Path("/records/{ID}/representations/{REPRESENTATION}/versions/{VERSION}/files/")
@Component
public class FilesResource {

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


    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(
            @FormDataParam("mimeType") String mimeType,
            @FormDataParam("fileName") String fileName,
            @FormDataParam("data") InputStream data)
            throws FileAlreadyExistsException, IOException, RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        if (fileName != null) {
            return Response.status(Response.Status.NOT_IMPLEMENTED).entity("If you want to provide your own file name, use: files/{FILE_NAME}").build();
        }
        if (data == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity("You must provide data.").build();
        }
        Representation rep = recordService.getRepresentation(globalId, representation, version);
        File f = new File();
        f.setMimeType(mimeType);
        contentService.insertContent(rep, f, data);
        URI fileUri = uriInfo.getAbsolutePathBuilder().path(f.getFileName()).build();
        return Response.created(fileUri).entity(mimeType).build();
    }
}
