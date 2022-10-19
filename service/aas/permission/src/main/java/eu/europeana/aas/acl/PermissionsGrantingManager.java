package eu.europeana.aas.acl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.*;

import java.util.List;

/**
 * Grants permissions to eCLoud objects (versions, files, data-providers, ...)
 */
public class PermissionsGrantingManager {


    @Autowired
    private MutableAclService mutableAclService;

    /**
     * @param objectType object type
     * @param objectIdentifier object identifier
     * @param userName name of the user who will be granted to access resource
     * @param listOfPermissions list of permissions that will be added to resource
     */
    public void grantPermissions(String objectType, String objectIdentifier, String userName, List<Permission> listOfPermissions) {

        ObjectIdentity objectIdentity = new ObjectIdentityImpl(objectType, objectIdentifier);

        MutableAcl versionAcl;
        try {
            versionAcl = (MutableAcl) mutableAclService.readAclById(objectIdentity);
        } catch (NotFoundException ex) {
            versionAcl = mutableAclService.createAcl(objectIdentity);
        }

        for (Permission permission : listOfPermissions) {
            versionAcl.insertAce(versionAcl.getEntries().size(), permission, new PrincipalSid(userName), true);
        }

        mutableAclService.updateAcl(versionAcl);
    }

    public void removePermissions(String objectType, String objectIdentifier, String userName, List<Permission> listOfPermissions) {
        ObjectIdentity objectIdentity = new ObjectIdentityImpl(objectType, objectIdentifier);
        removePermissions(objectIdentity, userName, listOfPermissions);
    }

    public void removePermissions(ObjectIdentity objectIdentity, String userName, List<Permission> listOfPermissions) {

        MutableAcl objectAcl = (MutableAcl) mutableAclService.readAclById(objectIdentity);

        for (int i = objectAcl.getEntries().size() - 1; i >= 0; i--) {
            AccessControlEntry currentEntry = objectAcl.getEntries().get(i);
            if (currentEntry.getSid() instanceof PrincipalSid) {
                PrincipalSid s = (PrincipalSid) currentEntry.getSid();
                if (userName.equals(s.getPrincipal()) && isPermissionOnTheList(currentEntry.getPermission(), listOfPermissions)) {
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
