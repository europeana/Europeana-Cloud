package eu.europeana.cloud.contentserviceapi.rest;

import java.net.URI;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.RecordNotExistsException;
import eu.europeana.cloud.contentserviceapi.exception.RepresentationNotExistsException;
import eu.europeana.cloud.contentserviceapi.service.RecordService;
import eu.europeana.cloud.definitions.model.Representation;

/**
 * RepresentationResource
 */
@Path("/records/{ID}/representations/{REPRESENTATION}/versions/{VERSION}")
@Component
public class RepresentationVersionResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @Context
    private Request request;

    @PathParam("ID")
    private String globalId;

    @PathParam("REPRESENTATION")
    private String representation;

    @PathParam("VERSION")
    private String version;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response getRepresentationVersion()
            throws RecordNotExistsException, RepresentationNotExistsException {
        if (PathConstants.LATEST_VERSION_KEYWORD.equals(version)) {
            Representation representationInfo = recordService.getRepresentation(globalId, representation);
            EnrichUriUtil.enrich(uriInfo, representationInfo);
            if (representationInfo.getSelfUri() != null) {
                return Response.temporaryRedirect(representationInfo.getSelfUri()).build();
            } else {
                throw new RepresentationNotExistsException();
            }
        }
        return Response.ok(recordService.getRepresentation(globalId, representation, version)).build();
    }


    @DELETE
    public void deleteRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        recordService.deleteRepresentation(globalId, representation, version);
    }


    @POST
    @Path("/persist")
    public Response persistRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        Representation persistentVersion = recordService.persistRepresentation(globalId, representation, version);

        return Response.created(persistentVersion.getSelfUri()).build();
    }


    @POST
    @Path("/copy")
    public Response copyRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException {
        Representation rep = recordService.getRepresentation(globalId, representation, version);

        Representation copiedRep = recordService.createRepresentation(globalId, representation, rep.getDataProvider());
        // TODO: copy files
        return Response.created(copiedRep.getSelfUri()).build();
    }
}
