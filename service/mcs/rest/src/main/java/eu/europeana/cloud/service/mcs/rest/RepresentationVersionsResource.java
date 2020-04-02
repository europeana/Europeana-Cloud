package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;

/**
 * Resource to manage representation versions.
 */
@RestController
@RequestMapping("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions")
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
     * Lists all versions of record representation. Temporary versions will be
     * included in the returned list.
     * @summary get all representation versions.
     *
     * @return list of all the representation versions.
     * @throws RepresentationNotExistsException representation does not exist.
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("java.util.List<eu.europeana.cloud.common.model.Representation>")
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
