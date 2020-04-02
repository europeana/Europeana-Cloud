package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;


/**
 * Resource to manage representations.
 */
@RestController
@RequestMapping("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/revisions/{" + P_REVISION_NAME + "}")
@Scope("request")
public class RepresentationRevisionsResource {

    @Autowired
    private RecordService recordService;

    @Autowired
    private PermissionEvaluator permissionEvaluator;

    /**
     * Returns the representation version which associates cloud identifier, representation name with revision identifier, provider and timestamp.
     * <strong>Read permissions required.</strong>
     *
     * @param globalId           cloud id of the record which contains the representation .
     * @param schema             name of the representation .
     * @param revisionName       name of the revision associated with this representation version
     * @param revisionProviderId identifier of institution that provided the revision
     * @param revisionTimestamp  timestamp of the specific revision, if not given the latest revision with revisionName
     *                           created by revisionProviderId will be considered (timestamp should be given in UTC format)
     * @return requested specific representation object.
     * @throws RepresentationNotExistsException when representation doesn't exist
     * @summary get a representation response object
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("java.util.List<eu.europeana.cloud.common.model.Representation>")
    public List<Representation> getRepresentationRevisions(@Context UriInfo uriInfo,
                                                           @PathParam(P_CLOUDID) String globalId,
                                                           @PathParam(P_REPRESENTATIONNAME) String schema,
                                                           @PathParam(P_REVISION_NAME) String revisionName,
                                                           @QueryParam(F_REVISION_PROVIDER_ID) String revisionProviderId,
                                                           @QueryParam(F_REVISION_TIMESTAMP) String revisionTimestamp) throws RepresentationNotExistsException {
        if (revisionProviderId == null) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorInfo("OTHER", F_REVISION_PROVIDER_ID + " parameter cannot be empty."))
                    .build());
        }
        Date revisionDate = null;
        if (revisionTimestamp != null) {
            DateTime utc = new DateTime(revisionTimestamp, DateTimeZone.UTC);
            revisionDate = utc.toDate();
        }
        List<RepresentationRevisionResponse> info = recordService.getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, revisionDate);
        List<Representation> representations = new ArrayList<>();
        if (info != null) {
            for (RepresentationRevisionResponse representationRevisionsResource : info) {
                Representation representation;
                representation = recordService.getRepresentation(
                        representationRevisionsResource.getCloudId(),
                        representationRevisionsResource.getRepresentationName(),
                        representationRevisionsResource.getVersion());
                EnrichUriUtil.enrich(uriInfo, representation);
                //
                if (userHasAccessTo(representation)) {
                    representations.add(representation);
                }
            }
        } else
            throw new RepresentationNotExistsException("No representation was found");

        return representations;
    }

    private boolean userHasAccessTo(Representation representation){
        SecurityContext ctx = SecurityContextHolder.getContext();
        Authentication authentication = ctx.getAuthentication();
        //
        String targetId = representation.getCloudId() + "/" + representation.getRepresentationName() + "/" + representation.getVersion();
        return permissionEvaluator.hasPermission(authentication, targetId, Representation.class.getName(), "read");
    }

}
