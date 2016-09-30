package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.*;


/**
 * Resource to manage representations.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME
		+ "}/revisions/{" + REVISION_NAME +"}")
@Component
@Scope("request")
public class RepresentationRevisionsResource {

	@Autowired
	private RecordService recordService;


	/**
	 * Returns the representation revision object which associates cloud identifier, representation name and version identifier with its revision identifier.
	 * <strong>Read permissions required.</strong>
	 *
	 * @summary get a representation
	 * @param globalId cloud id of the record which contains the representation .
	 * @param schema name of the representation .
	 * @param revisionId identifier of the revision associated with this representation version
	 *
	 * @return requested representation revision object.
	 * @throws RepresentationNotExistsException
	 *             representation does not exist or no persistent version of
	 *             this representation exists.
	 */
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @PostAuthorize("hasPermission"
    	    + "( "
    	    + " (#globalId).concat('/').concat(#schema).concat('/').concat(returnObject.version) ,"
    	    + " 'eu.europeana.cloud.common.model.Representation', read" + ")")
	@ReturnType("eu.europeana.cloud.common.response.RepresentationRevisionResponse")
	public RepresentationRevisionResponse getRepresentationRevision(@Context UriInfo uriInfo,
																	@PathParam(P_CLOUDID) String globalId,
																	@PathParam(P_REPRESENTATIONNAME) String schema,
																	@PathParam(REVISION_NAME) String revisionId,
																	@NotNull @QueryParam(REVISION_PROVIDER_ID) String revisionProviderId)
			throws RevisionNotExistsException {

		if (revisionProviderId == null || revisionProviderId.isEmpty())
			return null;

		RepresentationRevisionResponse info = recordService.getRepresentationRevision(globalId, schema, RevisionUtils.getRevisionKey(revisionProviderId, revisionId));
		EnrichUriUtil.enrich(uriInfo, info);
		return info;
	}
}
