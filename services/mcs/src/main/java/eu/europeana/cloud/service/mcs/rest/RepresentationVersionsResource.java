package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.service.RecordService;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

/**
 * RepresentationVersionsResource
 */
@Path("/records/{ID}/representations/{REPRESENTATION}/versions")
@Component
public class RepresentationVersionsResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_REP)
    private String representation;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<Representation> listVersions()
            throws RecordNotExistsException, RepresentationNotExistsException {
        List<Representation> representationVersions = recordService.listRepresentationVersions(globalId, representation);
        for (Representation representationVersion : representationVersions) {
            prepare(representationVersion);
        }
        return representationVersions;
    }


    private void prepare(Representation representationVersion) {
        EnrichUriUtil.enrich(uriInfo, representationVersion);
    }
}
