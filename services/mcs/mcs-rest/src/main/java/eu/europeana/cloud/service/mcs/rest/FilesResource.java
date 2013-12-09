package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_FILE_DATA;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_FILE_MIME;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_GID;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_SCHEMA;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_VER;
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

/**
 * FilesResource
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_SCHEMA + "}/versions/{" + P_VER + "}/files")
@Component
public class FilesResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_SCHEMA)
    private String schema;

    @PathParam(P_VER)
    private String version;


    // the same as method in FileResource. Duplicated code.

    @Deprecated
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(@FormDataParam(F_FILE_MIME) String mimeType, @FormDataParam(F_FILE_DATA) InputStream data)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        ParamUtil.require(F_FILE_DATA, data);

        File f = new File();
        f.setMimeType(mimeType);

        recordService.putContent(globalId, schema, version, f, data);

        EnrichUriUtil.enrich(uriInfo, globalId, schema, version, f);
        return Response.created(f.getContentUri()).tag(f.getMd5()).build();
    }
}
