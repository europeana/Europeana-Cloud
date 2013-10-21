package eu.europeana.cloud.service.mcs.rest;

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

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.service.RecordService;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

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

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_REP)
    private String representation;

    @PathParam(P_VER)
    private String version;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response getRepresentationVersion()
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
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
    public Response deleteRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        recordService.deleteRepresentation(globalId, representation, version);
        return Response.noContent().build();
    }


    @POST
    @Path("/persist")
    public Response persistRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        Representation persistentVersion = recordService.persistRepresentation(globalId, representation, version);

        return Response.created(persistentVersion.getSelfUri()).build();
    }


    @POST
    @Path("/copy")
    public Response copyRepresentation()
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, ProviderNotExistsException {
        Representation rep = recordService.getRepresentation(globalId, representation, version);

        Representation copiedRep = recordService.createRepresentation(globalId, representation, rep.getDataProvider());
        // TODO: copy files
        return Response.created(copiedRep.getSelfUri()).build();
    }
}
