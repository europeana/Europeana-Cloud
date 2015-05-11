package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.AASParamConstants.P_USER_NAME;
import static eu.europeana.cloud.common.web.AASParamConstants.P_PERMISSION;
import static eu.europeana.cloud.common.web.ParamConstants.F_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.F_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_VER;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;

/**
 * Resource to authorize other users to read specific versions.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}")
@Component
public class RepresentationAuthorizationResource {

    @Autowired
    private MutableAclService mutableAclService;

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationAuthorizationResource.class);

    /**
     * Modify authorization of versions operation. Updates authorization. for a
     * specific representation version.
     *
     * @param globalId cloud id of the record (required).
     * @param schema   schema of representation (required).
     * @param version  a specific version of the representation(required).
     * @param username  username as part of credential (required).
     * @param permission permission as part of credential (required).
     * @return response tells you if authorization has been updated or not
     * @summary update authorization for a representation version
     * @statuscode 204 object has been updated.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/users/{" + P_USER_NAME + "}/permit/{" + P_PERMISSION + "}")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public Response updateAuthorization(
            @PathParam(P_CLOUDID) String globalId,
            @PathParam(P_REPRESENTATIONNAME) String schema,
            @PathParam(P_VER) String version,
            @PathParam(P_USER_NAME) String username,
            @PathParam(P_PERMISSION) String permission
    ) {

        ParamUtil.require(P_CLOUDID, globalId);
        ParamUtil.require(P_REPRESENTATIONNAME, schema);
        ParamUtil.require(P_VER, version);
        ParamUtil.require(P_USER_NAME, username);
        ParamUtil.require(P_PERMISSION, permission);

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);

        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        if (versionAcl != null) {

            try {
                int pAsInt = Integer.parseInt(permission);
                if (pAsInt == BasePermission.READ.getMask()) {
                    versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, new PrincipalSid(username), true);
                }
                if (pAsInt == BasePermission.WRITE.getMask()) {
                    versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, new PrincipalSid(username), true);
                    versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.WRITE, new PrincipalSid(username), true);
                }
                mutableAclService.updateAcl(versionAcl);
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
                return Response.notModified("Authorization has NOT been updated!").build();
            }
            return Response.ok("Authorization has been updated!").build();
        } else {
            return Response.notModified("Authorization has NOT been updated!").build();
        }
    }

    /**
     * Gives read access to everyone (even anonymous users) for the specified File.
     *
     * @param globalId cloud id of the record (required).
     * @param schema   schema of representation (required).
     * @param version  a specific version of the representation(required).
     * @return response tells you if authorization has been updated or not
     * @statuscode 204 object has been updated.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    @Path("/permit")
    public Response giveReadAccessToEveryone(
            @PathParam(P_CLOUDID) String globalId,
            @PathParam(P_REPRESENTATIONNAME) String schema,
            @PathParam(P_VER) String version) {

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);

        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        Sid anonRole = new GrantedAuthoritySid("ROLE_ANONYMOUS");
        Sid userRole = new GrantedAuthoritySid("ROLE_USER");

        if (versionAcl != null) {
            versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, anonRole, true);
            versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, userRole, true);
            mutableAclService.updateAcl(versionAcl);
            return Response.ok("Authorization has been updated!").build();
        } else {
            return Response.notModified("Authorization has NOT been updated!").build();
        }
    }
}
