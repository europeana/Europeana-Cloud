package eu.europeana.cloud.contentserviceapi.rest;

import java.net.URI;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.RecordNotExistsException;
import eu.europeana.cloud.contentserviceapi.exception.RepresentationNotExistsException;
import eu.europeana.cloud.contentserviceapi.model.Representation;
import eu.europeana.cloud.contentserviceapi.service.RecordService;

import static eu.europeana.cloud.contentserviceapi.rest.PathConstants.*;

/**
 * RepresentationResource
 */
@Path("/records/{ID}/representations/{REPRESENTATION}")
@Component
public class RepresentationResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @Context
    private Request request;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_REP)
    private String representation;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Representation getRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        Representation info = recordService.getRepresentation(globalId, representation);
        prepare(info);
        return info;
    }


    @DELETE
    public void deleteRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        recordService.deleteRepresentation(globalId, representation);
    }


    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createRepresentation(
            @FormParam(P_PROVIDER) String providerId)
            throws RecordNotExistsException, RepresentationNotExistsException {
        if (providerId == null || providerId.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Representation version = recordService.createRepresentation(globalId, representation, providerId);
        EnrichUriUtil.enrich(uriInfo, version);
        return Response.created(version.getSelfUri()).build();
    }


    private void prepare(Representation representation) {
        representation.setFiles(null);
        EnrichUriUtil.enrich(uriInfo, representation);
    }
}
