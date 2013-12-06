package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * RepresentationResource
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_SCHEMA + "}")
@Component
public class RepresentationResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_SCHEMA)
    private String schema;


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Representation getRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        Representation info = recordService.getRepresentation(globalId, schema);
        prepare(info);
        return info;
    }


    @DELETE
    public Response deleteRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        recordService.deleteRepresentation(globalId, schema);
        return Response.noContent().build();
    }


    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createRepresentation(@FormParam(F_PROVIDER) String providerId)
            throws RecordNotExistsException, RepresentationNotExistsException, ProviderNotExistsException {
        ParamUtil.require(F_PROVIDER, providerId);
        Representation version = recordService.createRepresentation(globalId, schema, providerId);
        EnrichUriUtil.enrich(uriInfo, version);
        return Response.created(version.getUri()).build();
    }


    private void prepare(Representation representation) {
        representation.setFiles(null);
        EnrichUriUtil.enrich(uriInfo, representation);
    }
}
