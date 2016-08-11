package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage representation versions.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}")
@Component
@Scope("request")
public class RepresentationVersionResource {

    @Autowired
    private RecordService recordService;

    @Autowired
    private MutableAclService mutableAclService;

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    /**
     * Returns representation in a specified version.
     * <strong>Read permissions required.</strong>
     *
     * @param globalId cloud id of the record which contains the representation(required).
     * @param schema   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @return representation in requested version
     * @throws RepresentationNotExistsException representation does not exist in the
     *                                          specified version.
     * @summary get representation by version
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    @ReturnType("eu.europeana.cloud.common.model.Representation")
    public Representation getRepresentationVersion(@Context UriInfo uriInfo,
                                                   @PathParam(P_VER) String version,
                                                   @PathParam(P_REPRESENTATIONNAME) String schema,
                                                   @PathParam(P_CLOUDID) String globalId)
            throws RepresentationNotExistsException {

        Representation rep = recordService.getRepresentation(globalId, schema, version);
        prepare(uriInfo, rep);
        return rep;
    }

    /**
     * Deletes representation version.
     * <strong>Delete permissions required.</strong>
     *
     * @param globalId cloud id of the record which contains the representation version (required).
     * @param schema   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @throws RepresentationNotExistsException              representation does not exist in
     *                                                       specified version.
     * @throws CannotModifyPersistentRepresentationException representation in
     *                                                       specified version is persistent and as such cannot be removed.
     */
    @DELETE
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version), 'eu.europeana.cloud.common.model.Representation', delete)")
    public void deleteRepresentation(@PathParam(P_VER) String version,
                                     @PathParam(P_REPRESENTATIONNAME) String schema,
                                     @PathParam(P_CLOUDID) String globalId)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        recordService.deleteRepresentation(globalId, schema, version);

        // let's delete the permissions as well
        ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);
        mutableAclService.deleteAcl(dataSetIdentity, false);
    }

    /**
     * Persists temporary representation.
     * <p/>
     * <strong>Write permissions required.</strong>
     *
     * @param globalId cloud id of the record which contains the representation version(required).
     * @param schema   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @return URI to the persisted representation in content-location.
     * @throws RepresentationNotExistsException              representation does not exist in
     *                                                       specified version.
     * @throws CannotModifyPersistentRepresentationException representation
     *                                                       version is already persistent.
     * @throws CannotPersistEmptyRepresentationException     representation version
     *                                                       has no file attached and as such cannot be made persistent.
     * @statuscode 201 representation is made persistent.
     */
    @POST
    @Path("/persist")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public Response persistRepresentation(@Context UriInfo uriInfo,
                                          @PathParam(P_VER) String version,
                                          @PathParam(P_REPRESENTATIONNAME) String schema,
                                          @PathParam(P_CLOUDID) String globalId)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException {
        Representation persistentVersion = recordService.persistRepresentation(globalId, schema, version);
        prepare(uriInfo, persistentVersion);
        return Response.created(persistentVersion.getUri()).build();
    }

    /**
     * Copies all information with all files and their contents from one
     * representation version to a new temporary one.
     * <strong>Read permissions required.</strong>
     *
     * @param globalId cloud id of the record which contains the representation version
     * @param schema   name of the representation
     * @param version  a specific version of the representation
     * @return URI to the created representation in content-location.
     * @throws RepresentationNotExistsException representation does not exist in
     *                                          specified version.
     * @summary copy information including file contents from one representation version to another
     * @statuscode 201 representation has been copied to a new one.
     */
    @POST
    @Path("/copy")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public Response copyRepresentation(@Context UriInfo uriInfo,
                                       @PathParam(P_VER) String version,
                                       @PathParam(P_REPRESENTATIONNAME) String schema,
                                       @PathParam(P_CLOUDID) String globalId)
            throws RepresentationNotExistsException {
        Representation copiedRep = recordService.copyRepresentation(globalId, schema, version);
        prepare(uriInfo, copiedRep);

        String copiedReprOwner = SpringUserUtils.getUsername();
        if (copiedReprOwner != null) {

            ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                    globalId + "/" + schema + "/" + copiedRep.getVersion());

            MutableAcl versionAcl = mutableAclService.createAcl(versionIdentity);

            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(copiedReprOwner), true);
            versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(copiedReprOwner), true);
            versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(copiedReprOwner), true);
            versionAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(copiedReprOwner),
                    true);

            mutableAclService.updateAcl(versionAcl);
        }

        return Response.created(copiedRep.getUri()).build();
    }

    private void prepare(UriInfo uriInfo, Representation representationVersion) {
        EnrichUriUtil.enrich(uriInfo, representationVersion);
    }
}
