package eu.europeana.aas.acl;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.ObjectIdentity;

/**
 * Authentication Service Unit tests
 *
 * @author Markus.Muhr@theeuropeanlibrary.org
 * @since Aug 07, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/default-context.xml"})
public class CassandraAclServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraMutableAclService mutableAclService;

    private final String creator = "markus";

    private final String testKey = CassandraAclServiceTest.class.getName();

    private final String testValue = "entry";

    /**
     * Prepare the unit tests
     */
    @Before
    public void prepare() {
        @SuppressWarnings("resource")
        ApplicationContext context = new ClassPathXmlApplicationContext("default-context.xml");
        mutableAclService = (CassandraMutableAclService) context.getBean("mutableAclService");
    }

    /**
     * Test creation and retrieving of user.
     *
     * @throws Exception expected = UserExistsException.class
     */
    @Test()
    public void testCreateAndRetrieve() throws Exception {
        ObjectIdentity obj = new ObjectIdentityImpl(testKey,
                testValue);

//        MutableAcl acl = mutableAclService.createAcl(obj);
//
//        acl.insertAce(0, BasePermission.READ, new PrincipalSid(creator), true);
//        acl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creator), true);
//        acl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creator), true);
//        acl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creator),
//                true);
//
//        mutableAclService.updateAcl(acl);
//
//        Acl readAcl = mutableAclService.readAclById(obj);
//        Assert.assertEquals(acl.getEntries().size(), readAcl.getEntries().size());
//
//        readAcl = mutableAclService.readAclById(new ObjectIdentityImpl(testKey,
//                creator));
//        Assert.assertNull(readAcl);
    }
}
