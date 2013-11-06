package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

/**
 * FilesResource
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_REP + "}/versions/{" + P_VER + "}/files")
@Component
public class FilesResource {

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


    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(
            @FormDataParam(F_FILE_MIME) String mimeType,
            @FormDataParam(F_FILE_DATA) InputStream data)
            throws IOException, RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        ParamUtil.require(F_FILE_DATA, data);

        File f = new File();
        f.setMimeType(mimeType);

        recordService.putContent(globalId, representation, version, f, data);

        EnrichUriUtil.enrich(uriInfo, globalId, representation, version, f);
        return Response.created(f.getContentUri()).tag(f.getMd5()).build();
    }
}
