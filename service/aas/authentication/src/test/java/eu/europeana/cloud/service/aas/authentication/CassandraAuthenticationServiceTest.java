package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

/**
 * Authentication Service Unit tests
 * 
 * @author Markus.Muhr@theeuropeanlibrary.org
 * @since Aug 07, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContextConfiguration.class)
public class CassandraAuthenticationServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraAuthenticationService service;
    @Autowired
    private CassandraConnectionProvider provider;
    @Autowired
    private CassandraUserDAO dao;

    /**
     * Test creation and retrieving of user.
     *
     * @throws Exception
     */
    @Test(expected = UserExistsException.class)
    public void testCreateAndRetrieve() throws Exception {
        User gU = new User("test", "test");
        service.createUser(gU);
        User gURet = service.getUser("test");
        assertEquals(gU.getUsername(), gURet.getUsername());
        assertEquals(gU.getPassword(), gURet.getPassword());
        service.createUser(gU);
    }

    /**
     * Test UserDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = UserDoesNotExistException.class)
    public void testUserDoesNotExist() throws Exception {
        service.getUser("test2");
    }

    /**
     * Test delete user
     *
     * @throws Exception
     */
    @Test(expected = UserDoesNotExistException.class)
    public void testDeleteUser() throws Exception {
        dao.createUser(new SpringUser("test3", "test3"));
        service.deleteUser("test3");
        service.getUser("test3");
    }

    /**
     * Test UserDoesNotExistException when deleting
     *
     * @throws Exception
     */
    @Test(expected = UserDoesNotExistException.class)
    public void testDeleteUserException() throws Exception {
        service.deleteUser("test4");
    }
}
