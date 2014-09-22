package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
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
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

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
     * Returns representation in specified version. If Version = LATEST, will
     * redirect to actual latest persistent version at the moment of invoking
     * this method.
     *
     * @return representation in requested version WITH LIST OF FILES IN THIS
     * REPRESENTATION
     * @throws RepresentationNotExistsException representation does not exist in
     * specified version.
     * @statuscode 307 if requested version is "LATEST" - redirect to latest
     * persistent version.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response getRepresentationVersion(@Context UriInfo uriInfo,
    		@PathParam(P_VER) String version,
    		@PathParam(P_REPRESENTATIONNAME) String schema,
    		@PathParam(P_CLOUDID) String globalId)
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
        prepare(uriInfo, rep);
        return Response.ok(rep).build();
    }

    /**
     * Deletes representation version.
     *
     * @throws RepresentationNotExistsException representation does not exist in
     * specified version.
     * @throws CannotModifyPersistentRepresentationException representation in
     * specified version is persitent and as such cannot be removed.
     */
    @DELETE
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version), 'eu.europeana.cloud.common.model.Representation', delete)")
    public void deleteRepresentation(@PathParam(P_VER) String version,
    		@PathParam(P_REPRESENTATIONNAME) String schema,
    		@PathParam(P_CLOUDID) String globalId)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        recordService.deleteRepresentation(globalId, schema, version);
    }

    /**
     * Persists temporary representation.
     *
     * @return URI to persisted representation in content-location
     * @throws RepresentationNotExistsException representation does not exist in
     * specified version.
     * @throws CannotModifyPersistentRepresentationException representation
     * version is already persistent.
     * @throws CannotPersistEmptyRepresentationException representation version
     * has no file attached and as such cannot be made persistent.
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
     * Copies all information with all files and their content from one
     * representation version to a new temporary one.
     *
     * @return uri to created representation in content-location
     * @throws RepresentationNotExistsException representation does not exist in
     * specified version.
     * @statuscode 201 representation has been copied to a new one.
     *
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
