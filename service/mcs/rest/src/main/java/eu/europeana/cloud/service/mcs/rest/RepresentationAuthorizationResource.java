package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.commons.permissions.PermissionsGrantingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.*;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.common.web.AASParamConstants.P_PERMISSION;
import static eu.europeana.cloud.common.web.AASParamConstants.P_USER_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to authorize other users to read specific versions.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}")
@Component
public class RepresentationAuthorizationResource {

    private static final List<String> acceptedPermissionValues
            = Arrays.asList(
            eu.europeana.cloud.common.model.Permission.ALL.getValue(),
            eu.europeana.cloud.common.model.Permission.READ.getValue(),
            eu.europeana.cloud.common.model.Permission.WRITE.getValue(),
            eu.europeana.cloud.common.model.Permission.ADMINISTRATION.getValue(),
            eu.europeana.cloud.common.model.Permission.DELETE.getValue());

    @Autowired
    private MutableAclService mutableAclService;

    @Autowired
    private PermissionsGrantingManager permissionsGrantingManager;

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationAuthorizationResource.class);

    /**
     * Removes permissions for selected user to selected representation version.<br/><br/>
     * Permissions option:<br/>
     * <li>all</li>
     * <li>read</li>
     * <li>write</li>
     * <li>delete</li>
     * <li>administration</li>
     *
     * @param globalId   cloud id of the record (required).
     * @param schema     schema of representation (required).
     * @param version    a specific version of the representation(required).
     * @param userName   username as part of credential (required).
     * @param permission permission as part of credential (required).
     * @return response tells you if authorization has been updated or not
     * @summary Permissions removal
     */
    @DELETE
    @Path("/permissions/{" + P_PERMISSION + "}/users/{" + P_USER_NAME + "}")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public Response removePermissions(@PathParam(P_CLOUDID) String globalId,
                                      @PathParam(P_REPRESENTATIONNAME) String schema,
                                      @PathParam(P_VER) String version,
                                      @PathParam(P_USER_NAME) String userName,
                                      @PathParam(P_PERMISSION) String permission) {


        ParamUtil.require(P_CLOUDID, globalId);
        ParamUtil.require(P_REPRESENTATIONNAME, schema);
        ParamUtil.require(P_VER, version);
        ParamUtil.require(P_USER_NAME, userName);
        ParamUtil.require(P_PERMISSION, permission);
        ParamUtil.validate(P_PERMISSION, permission, acceptedPermissionValues);

        eu.europeana.cloud.common.model.Permission _permission = eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);

        LOGGER.info("Removing privileges for user '{}' to  '{}' with key '{}'", userName, versionIdentity.getType(), versionIdentity.getIdentifier());

        List<Permission> permissionsToBeRemoved = Arrays.asList(_permission.getSpringPermissions());
        permissionsGrantingManager.removePermissions(versionIdentity, userName, permissionsToBeRemoved);

        return Response.noContent().build();
    }

    /**
     * Modify authorization of versions operation. Updates authorization for a
     * specific representation version.
     * <p/>
     * <strong>Write permissions required.</strong>
     *
     * @param globalId   cloud id of the record (required).
     * @param schema     schema of representation (required).
     * @param version    a specific version of the representation(required).
     * @param username   username as part of credential (required).
     * @param permission permission as part of credential (required).
     * @return response tells you if authorization has been updated or not
     * @summary update authorization for a representation version
     * @statuscode 200 object has been updated.
     * @statuscode 204 object has not been updated.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/permissions/{" + P_PERMISSION + "}/users/{" + P_USER_NAME + "}")
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
        ParamUtil.validate(P_PERMISSION, permission, acceptedPermissionValues);

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);
        eu.europeana.cloud.common.model.Permission _permission = eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());
        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        if (versionAcl != null) {
            try {
                final Permission[] permissions = _permission.getSpringPermissions();
                for (Permission singlePermission : permissions) {
                    versionAcl.insertAce(versionAcl.getEntries().size(), singlePermission, new PrincipalSid(username), true);
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
     * <p/>
     * <strong>Write permissions required.</strong>
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
