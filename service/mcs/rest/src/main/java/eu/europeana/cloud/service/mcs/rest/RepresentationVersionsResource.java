package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Resource to manage representation versions.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions")
@Component
@Scope("request")
public class RepresentationVersionsResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_CLOUDID)
    private String globalId;

    @PathParam(P_REPRESENTATIONNAME)
    private String representation;


    /**
     * Lists all versions of record represenation. Temporary versions will be included in returned list.
     * 
     * @return list of all representation versions
     * @throws RepresentationNotExistsException
     *             representation does not exist.
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public List<Representation> listVersions()
            throws RepresentationNotExistsException {
        List<Representation> representationVersions = recordService
                .listRepresentationVersions(globalId, representation);
        for (Representation representationVersion : representationVersions) {
            prepare(representationVersion);
        }
        return representationVersions;
    }


    private void prepare(Representation representationVersion) {
        EnrichUriUtil.enrich(uriInfo, representationVersion);
    }
}
