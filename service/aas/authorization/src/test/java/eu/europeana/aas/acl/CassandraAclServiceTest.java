package eu.europeana.aas.acl;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Authentication Service Unit tests
 *
 * @author Markus.Muhr@theeuropeanlibrary.org
 * @since Aug 07, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContextConfiguration.class)
public class CassandraAclServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraMutableAclService mutableAclService;

    private final String creator = "markus";

    private final String testKey = CassandraAclServiceTest.class.getName();

    private final String testValue = "entry";

    /**
     * Test creation and retrieving of user.
     *
     * @throws Exception expected = UserExistsException.class
     */
    @Test(expected = NotFoundException.class)
    public void testCreateAndRetrieve() throws Exception {
        TestingAuthenticationToken auth = new TestingAuthenticationToken(creator, creator);
        auth.setAuthenticated(true);
        SecurityContextHolder.getContext().setAuthentication(auth);

        ObjectIdentity obj = new ObjectIdentityImpl(testKey,
                testValue);

        MutableAcl acl = mutableAclService.createAcl(obj);

        acl.insertAce(0, BasePermission.READ, new PrincipalSid(creator), true);
        acl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creator), true);
        acl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creator), true);
        acl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creator),
                true);

        mutableAclService.updateAcl(acl);

        Acl readAcl = mutableAclService.readAclById(obj);
        Assert.assertTrue(acl.getEntries().size() == readAcl.getEntries().size());

        mutableAclService.readAclById(new ObjectIdentityImpl(testKey,
                creator));
    }
}
