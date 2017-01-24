package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.Date;

import static eu.europeana.cloud.common.web.ParamConstants.*;


/**
 * Resource to manage representations.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME
		+ "}/revisions/{" + P_REVISION_NAME +"}")
@Component
@Scope("request")
public class RepresentationRevisionsResource {

	@Autowired
	private RecordService recordService;


	/**
	 * Returns the representation revision object which associates cloud identifier, representation name and version identifier with its revision identifier.
	 * <strong>Read permissions required.</strong>
	 *
	 * @summary get a representation revision response object
	 * @param globalId cloud id of the record which contains the representation .
	 * @param schema name of the representation .
	 * @param revisionName name of the revision associated with this representation version
	 * @param revisionProviderId identifier of institution that provided the revision
	 * @param revisionTimestamp timestamp of the specific revision, if not given the latest revision with revisionName
	 *                          created by revisionProviderId will be considered (timestamp should be given in UTC format)
	 *
	 * @return requested representation revision object.
	 */
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @PostAuthorize("returnObject.version != null ? hasPermission"
    	    + "( "
    	    + " (#globalId).concat('/').concat(#schema).concat('/').concat(returnObject.version) ,"
    	    + " 'eu.europeana.cloud.common.model.Representation', read" + ") : true")
	@ReturnType("eu.europeana.cloud.common.response.RepresentationRevisionResponse")
	public RepresentationRevisionResponse getRepresentationRevision(@Context UriInfo uriInfo,
																	@PathParam(P_CLOUDID) String globalId,
																	@PathParam(P_REPRESENTATIONNAME) String schema,
																	@PathParam(P_REVISION_NAME) String revisionName,
																	@QueryParam(F_REVISION_PROVIDER_ID) String revisionProviderId,
																	@QueryParam(F_REVISION_TIMESTAMP) String revisionTimestamp) {
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
		RepresentationRevisionResponse info = recordService.getRepresentationRevision(globalId, schema, revisionProviderId, revisionName, revisionDate);
		if (info != null) {
			EnrichUriUtil.enrich(uriInfo, info);
		}
		else {
			// create empty response object
			info = new RepresentationRevisionResponse();

			try {
				// retrieve record to whether this is the cause of non-existent revision
				recordService.getRecord(globalId);
				// retrieve representation to check whether this is the cause of non-existent revision
				recordService.getRepresentation(globalId, schema);
			} catch (RepresentationNotExistsException e) {
				throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).entity(new ErrorInfo("REPRESENTATION_NOT_EXISTS", "Representation " + schema + " does not exist.")).build());
			} catch (RecordNotExistsException e) {
				throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).entity(new ErrorInfo("RECORD_NOT_EXISTS", "Record " + globalId + " does not exist.")).build());
			}
		}
		return info;
	}
}
