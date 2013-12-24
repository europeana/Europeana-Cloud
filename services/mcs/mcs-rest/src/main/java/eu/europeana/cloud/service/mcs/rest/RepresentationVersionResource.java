package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.common.web.ParamConstants.P_GID;
import static eu.europeana.cloud.common.web.ParamConstants.P_SCHEMA;
import static eu.europeana.cloud.common.web.ParamConstants.P_VER;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Resource to manage representation versions.
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_SCHEMA + "}/versions/{" + P_VER + "}")
@Component
@Scope("request")
public class RepresentationVersionResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_SCHEMA)
    private String schema;

    @PathParam(P_VER)
    private String version;


    /**
     * Returns representation in specified version. If Version = LATEST, will redirect to actual latest persistent
     * version at the moment of invoking this method. *
     * 
     * @return representation in requested version WITH LIST OF FILES IN THIS REPRESENTATION
     * @throws RepresentationNotExistsException
     *             representation does not * exist in specified version.
     * @statuscode 307 if requested version is "LATEST" - redirect to latest persistent version.
     * */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Response getRepresentationVersion()
            throws RepresentationNotExistsException {
        // handle "LATEST" keyword
        if (ParamConstants.LATEST_VERSION_KEYWORD.equals(version)) {
            Representation representationInfo = recordService.getRepresentation(globalId, schema);
            EnrichUriUtil.enrich(uriInfo, representationInfo);
            if (representationInfo.getUri() != null) {
                return Response.temporaryRedirect(representationInfo.getUri()).build();
            } else {
                throw new RepresentationNotExistsException();
            }
        }
        Representation rep = recordService.getRepresentation(globalId, schema, version);
        prepare(rep);
        return Response.ok(rep).build();
    }


    /**
     * Deletes representation version.
     * 
     * @throws RepresentationNotExistsException
     *             representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException
     *             representation in specified version is persitent and as such cannot be removed.
     */
    @DELETE
    public void deleteRepresentation()
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        recordService.deleteRepresentation(globalId, schema, version);
    }


    /**
     * Persists temporary representation.
     * 
     * @return URI to persisted representation in content-location
     * @throws RepresentationNotExistsException
     *             * representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException
     *             representation version is already persistent.
     * @throws CannotPersistEmptyRepresentationException
     *             representation version has no file attached and as such cannot be made persistent.
     * @statuscode 201 representation is made persistent.
     */
    @POST
    @Path("/persist")
    public Response persistRepresentation()
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException {
        Representation persistentVersion = recordService.persistRepresentation(globalId, schema, version);
        prepare(persistentVersion);
        return Response.created(persistentVersion.getUri()).build();
    }


    /**
     * Copies all information with all files and their content from one representation version to a new temporary one. *
     * 
     * @return uri to created representation in content-location
     * @throws RepresentationNotExistsException
     *             * representation does not exist in specified version.
     * @statuscode 201 representation has been copied to a new one.
     * */
    @POST
    @Path("/copy")
    public Response copyRepresentation()
            throws RepresentationNotExistsException {
        Representation copiedRep = recordService.copyRepresentation(globalId, schema, version);
        prepare(copiedRep);
        return Response.created(copiedRep.getUri()).build();
    }


    private void prepare(Representation representationVersion) {
        EnrichUriUtil.enrich(uriInfo, representationVersion);
    }
}
