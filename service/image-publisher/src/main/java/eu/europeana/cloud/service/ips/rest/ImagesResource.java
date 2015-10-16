package eu.europeana.cloud.service.ips.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.service.ips.ImageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response;

import java.lang.annotation.Annotation;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource that represents images URI translator
 */
@Path("/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME
        + "}/versions/{" + P_VER + "}/files/{" + P_FILENAME + "}/manifest")
@Component
@Scope("request")
public class ImagesResource {

    private ImageTranslator translator;

    /**
     * Returns manifest file in json format describing the image associated with the specified version.
     *
     * @param globalId cloud id of the record in which the version exists
     * @param schema   representation name
     * @param version  version name
     * @param fileName file name
     * @return manifest file for image.
     * @summary get manifest file for image
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @ReturnType("javax.ws.rs.core.Response")
    public Response getManifest(@Context UriInfo uriInfo, @PathParam(P_CLOUDID) String globalId, @PathParam(P_REPRESENTATIONNAME) String schema, @PathParam(P_VER) String version, @PathParam(P_FILENAME) String fileName) {

        // call IIP Image Server for manifest file in json format
        String response = translator.getResponse(globalId, schema, version, fileName);
        if (response != null)
            return Response.ok().entity(response).build();
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    public void setTranslator(ImageTranslator translator) {
        this.translator = translator;
    }
}
