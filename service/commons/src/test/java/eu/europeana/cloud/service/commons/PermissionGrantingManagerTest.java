package eu.europeana.cloud.service.commons;

import eu.europeana.cloud.service.commons.cassandra.CassandraTestBase;
import eu.europeana.cloud.service.commons.permissions.PermissionsGrantingManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/default-context.xml",
        "classpath:authentication-context-test.xml"})
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

    @Before
    public void init() {
        Authentication auth = new UsernamePasswordAuthenticationToken(ADMIN_NAME, ADMIN_PASSWORD);
        SecurityContextHolder.getContext().setAuthentication(authenticationManager.authenticate(auth));
    }

    //////////////////////
    // adding permissions
    //////////////////////
    @Test
    public void readPermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));

        Acl acl = readAcl();

        Assert.assertTrue(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
    }

    @Test
    public void writePermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));

        Acl acl = readAcl();

        Assert.assertTrue(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

    @Test
    public void createPermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));

        Acl acl = readAcl();

        Assert.assertTrue(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

    @Test
    public void deletePermissionShouldBeGranted() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));

        Acl acl = readAcl();

        Assert.assertTrue(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
    }

    //////////////////////
    // removing permissions
    //////////////////////

    @Test
    public void readPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));

        Acl acl = readAcl();

        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    public void writePermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.WRITE));
        
        Acl acl = readAcl();

        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    public void createPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.CREATE));

        Acl acl = readAcl();

        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.CREATE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    public void deletePermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        Acl acl = readAcl();

        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
    }

    @Test
    public void notExistingPermissionShouldBeRemoved() {
        permissionsGrantingManager.grantPermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.DELETE));
        permissionsGrantingManager.removePermissions(OBJECT_TYPE, OBJECT_ID, USER_NAME, Arrays.asList(BasePermission.READ));
        Acl acl = readAcl();

        Assert.assertFalse(isPermissionOnTheList(BasePermission.READ, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.WRITE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.DELETE, acl.getEntries()));
        Assert.assertFalse(isPermissionOnTheList(BasePermission.ADMINISTRATION, acl.getEntries()));
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
