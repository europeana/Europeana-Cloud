package eu.europeana.cloud.service.commons.permissions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;

import java.util.List;

/**
 * Grants permissions to eCLoud objects (versions, files, data-providers, ...)
 */
public class PermissionsGrantingManager {


    @Autowired
    private MutableAclService mutableAclService;

    /**
     * @param objectType
     * @param objectIdentifier
     * @param userName
     * @param listOfPermissions
     */
    public void grantPermissions(String objectType, String objectIdentifier, String userName, List<Permission> listOfPermissions) {

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(objectType, objectIdentifier);

        MutableAcl versionAcl = null;
        try {
            versionAcl = (MutableAcl) mutableAclService.readAclById(objectIdentity);
        } catch (NotFoundException ex) {
            versionAcl = mutableAclService.createAcl(objectIdentity);
        }

        for (Permission permission : listOfPermissions) {
            versionAcl.insertAce(versionAcl.getEntries().size(), permission, new PrincipalSid(userName), true);
        }
    }

    public void removePermissions(String objectType, String objectIdentifier, String userName, List<Permission> listOfPermissions) {
        ObjectIdentity objectIdentity = new ObjectIdentityImpl(objectType, objectIdentifier);
        removePermissions(objectIdentity, userName, listOfPermissions);
    }

    public void removePermissions(ObjectIdentity objectIdentity, String userName, List<Permission> listOfPermissions) {

        MutableAcl objectAcl = (MutableAcl) mutableAclService.readAclById(objectIdentity);

        for (int i = objectAcl.getEntries().size()-1; i > 0; i--) {
            AccessControlEntry currentEntry = objectAcl.getEntries().get(i);
            PrincipalSid s = (PrincipalSid) currentEntry.getSid();
            if (userName.equals(s.getPrincipal())) {
                if(isPermissionOnTheList(currentEntry.getPermission(),listOfPermissions)){
                    objectAcl.deleteAce(i);
                }
            }
        }
        mutableAclService.updateAcl(objectAcl);
    }

    private boolean isPermissionOnTheList(Permission permission, List<Permission> listOfPermissions) {
        for (Permission permissionFromList : listOfPermissions) {
            if (permissionFromList.equals(permission)) {
                return true;
            }
        }
        return false;
    }
}
