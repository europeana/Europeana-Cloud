package eu.europeana.cloud.service.ips.rest;

import com.qmino.miredot.annotations.ReturnType;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Scanner;

import static eu.europeana.cloud.common.web.ParamConstants.*;

@Path("/view/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME
        + "}/versions/{" + P_VER + "}/files/{" + P_FILENAME + ":(.+)?}")
@Component
@Scope("request")
public class ViewResource {

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
    @Produces({MediaType.TEXT_HTML})
    @ReturnType("javax.ws.rs.core.Response")
    public Response getView(@Context UriInfo uriInfo, @PathParam(P_CLOUDID) String globalId, @PathParam(P_REPRESENTATIONNAME) String schema, @PathParam(P_VER) String version, @PathParam(P_FILENAME) String fileName) {

        Scanner in = new Scanner(getClass().getResourceAsStream("/viewer.html")).useDelimiter("\\Z");
        String htmlBody = "";
        if (in.hasNext())
            htmlBody = in.next();
        if (htmlBody != null && !htmlBody.isEmpty())
            return Response.ok().entity(prepareManifestURL(htmlBody, uriInfo.getRequestUri().toString())).build();
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    private String prepareManifestURL(String htmlBody, String url) {
        return htmlBody.replace("$1", url.replace("/view/records", "/manifest/records"));
    }
}
