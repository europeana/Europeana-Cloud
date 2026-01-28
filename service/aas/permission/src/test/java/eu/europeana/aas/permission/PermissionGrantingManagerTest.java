package eu.europeana.aas.permission;

import eu.europeana.aas.permission.cassandra.CassandraTestBase;
import eu.europeana.aas.permission.config.AuthenticationTestContext;
import eu.europeana.aas.permission.config.DefaultTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.*;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {
        AuthenticationTestContext.class,
        DefaultTestContext.class
})
public class PermissionGrantingManagerTest extends CassandraTestBase {

    @Autowired
    private MutableAclService mutableAclService;

    @Autowired
  private PermissionsGrantingManager permissionsGrantingManager;

  @Autowired
  @Qualifier("authenticationManager")
  private AuthenticationManager authenticationManager;

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";

    private static final String OBJECT_TYPE = "representation";
    private static final String OBJECT_ID = "identifier";
    private static final String USER_NAME = "sampleUserName";

    @BeforeEach
    void init() {
        Authentication auth = new UsernamePasswordAuthenticationToken(ADMIN_NAME, ADMIN_PASSWORD);
        SecurityContextHolder.getContext().setAuthentication(authenticationManager.authenticate(auth));
    }

    //////////////////////
    // adding permissions
    //////////////////////
    @Test
    void readPermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));

        Acl acl = readAcl();

        assertTrue(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
    }

    @Test
    void writePermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));

        Acl acl = readAcl();

        assertTrue(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

    @Test
    void createPermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));

        Acl acl = readAcl();

        assertTrue(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

    @Test
    void deletePermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));

        Acl acl = readAcl();

        assertTrue(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

  //////////////////////
  // removing permissions
  //////////////////////

    @Test
    void readPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));

        Acl acl = readAcl();

        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    void writePermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));

        Acl acl = readAcl();

        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    void createPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));

        Acl acl = readAcl();

        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    void deletePermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        Acl acl = readAcl();

        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    void notExistingPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        Acl acl = readAcl();

        assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

  private Acl readAcl() {
    ObjectIdentity objectIdentity = new ObjectIdentityImpl(OBJECT_TYPE, OBJECT_ID);
    return mutableAclService.readAclById(objectIdentity);
  }

  private boolean isPermissionOnTheList(Permission permission, List<AccessControlEntry> entries) {
    for (AccessControlEntry entry : entries) {
      if (entry.getPermission() == permission) {
        return true;
      }
    }
    return false;

  }

}
