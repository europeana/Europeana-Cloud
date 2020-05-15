package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.commons.permissions.PermissionsGrantingManager;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.*;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to authorize other users to read specific versions.
 */
@RestController
@RequestMapping("/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/versions/{"+VERSION+"}")
public class RepresentationAuthorizationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationAuthorizationResource.class);

    private static final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    private static final List<String> ACCEPTED_PERMISSION_VALUES = Arrays.asList(
            eu.europeana.cloud.common.model.Permission.ALL.getValue(),
            eu.europeana.cloud.common.model.Permission.READ.getValue(),
            eu.europeana.cloud.common.model.Permission.WRITE.getValue(),
            eu.europeana.cloud.common.model.Permission.ADMINISTRATION.getValue(),
            eu.europeana.cloud.common.model.Permission.DELETE.getValue()
    );
    public static final String NO_UPDATED_MESSAGE = "Authorization has NOT been updated!";

    @Autowired
    private MutableAclService mutableAclService;

    @Autowired
    private PermissionsGrantingManager permissionsGrantingManager;

    /**
     * Removes permissions for selected user to selected representation version.<br/><br/>
     * Permissions option:<br/>
     * <li>all</li>
     * <li>read</li>
     * <li>write</li>
     * <li>delete</li>
     * <li>administration</li>
     *
     * @param cloudId   cloud id of the record (required).
     * @param representationName  schema of representation (required).
     * @param version    a specific version of the representation(required).
     * @param userName   username as part of credential (required).
     * @param permission permission as part of credential (required).
     * @return response tells you if authorization has been updated or not
     * @summary Permissions removal
     */

    @DeleteMapping(value = "/permissions/{"+PERMISSION+"}/users/{"+USER_NAME+"}")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public ResponseEntity<?> removePermissions(
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @PathVariable(VERSION) String version,
            @PathVariable(PERMISSION) String permission,
            @PathVariable(USER_NAME) String userName) {

        ParamUtil.validate("permission", permission, ACCEPTED_PERMISSION_VALUES);

        eu.europeana.cloud.common.model.Permission permissionUpperCase = eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                cloudId + "/" + representationName + "/" + version);

        LOGGER.info("Removing privileges for user '{}' to  '{}' with key '{}'", userName, versionIdentity.getType(), versionIdentity.getIdentifier());

        List<Permission> permissionsToBeRemoved = Arrays.asList(permissionUpperCase.getSpringPermissions());
        permissionsGrantingManager.removePermissions(versionIdentity, userName, permissionsToBeRemoved);

        return ResponseEntity.noContent().build();
    }

    /**
     * Modify authorization of versions operation. Updates authorization for a
     * specific representation version.
     * <p/>
     * <strong>Write permissions required.</strong>
     *
     * @param cloudId   cloud id of the record (required).
     * @param representationName schema of representation (required).
     * @param version    a specific version of the representation(required).
     * @param userName   username as part of credential (required).
     * @param permission permission as part of credential (required).
     * @return response tells you if authorization has been updated or not
     * @summary update authorization for a representation version
     * @statuscode 200 object has been updated.
     * @statuscode 204 object has not been updated.
     */
    @PostMapping(value = "/permissions/{permission}/users/{userName}")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public ResponseEntity<String> updateAuthorization(
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @PathVariable(VERSION) String version,
            @PathVariable(PERMISSION) String permission,
            @PathVariable(USER_NAME) String userName) {

        ParamUtil.validate("permission", permission, ACCEPTED_PERMISSION_VALUES);

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                cloudId + "/" + representationName + "/" + version);
        eu.europeana.cloud.common.model.Permission euPermission = eu.europeana.cloud.common.model.Permission.valueOf(permission.toUpperCase());
        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        if (versionAcl != null) {
            try {
                final Permission[] permissions = euPermission.getSpringPermissions();
                for (Permission singlePermission : permissions) {
                    versionAcl.insertAce(versionAcl.getEntries().size(), singlePermission, new PrincipalSid(userName), true);
                }
                mutableAclService.updateAcl(versionAcl);
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
                return ResponseEntity
                        .status(HttpStatus.NOT_MODIFIED)
                        .body(NO_UPDATED_MESSAGE);
            }
            return ResponseEntity.ok("Authorization has been updated!");
        } else {
            return ResponseEntity
                    .status(HttpStatus.NOT_MODIFIED)
                    .body(NO_UPDATED_MESSAGE);
        }
    }

    /**
     * Gives read access to everyone (even anonymous users) for the specified File.
     * <p/>
     * <strong>Write permissions required.</strong>
     *
     * @param cloudId cloud id of the record (required).
     * @param representationName   schema of representation (required).
     * @param version  a specific version of the representation(required).
     * @return response tells you if authorization has been updated or not
     * @statuscode 204 object has been updated.
     */
    @PostMapping(value = "/permit")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', write)")
    public ResponseEntity<String> giveReadAccessToEveryone(
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @PathVariable(VERSION) String version) {

        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                cloudId + "/" + representationName + "/" + version);

        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        Sid anonRole = new GrantedAuthoritySid("ROLE_ANONYMOUS");
        Sid userRole = new GrantedAuthoritySid("ROLE_USER");

        if (versionAcl != null) {
            versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, anonRole, true);
            versionAcl.insertAce(versionAcl.getEntries().size(), BasePermission.READ, userRole, true);
            mutableAclService.updateAcl(versionAcl);
            return ResponseEntity.ok("Authorization has been updated!");
        } else {
            return ResponseEntity
                    .status(HttpStatus.NOT_MODIFIED)
                    .body("Authorization has NOT been updated!");
        }
    }
}
