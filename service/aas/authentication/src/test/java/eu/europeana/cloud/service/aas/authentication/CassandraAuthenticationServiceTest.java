package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Authentication Service Unit tests
 *
 * @author Markus.Muhr@theeuropeanlibrary.org
 * @since Aug 07, 2014
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestContextConfiguration.class)
class CassandraAuthenticationServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraAuthenticationService service;
    @Autowired
    private CassandraConnectionProvider provider;
    @Autowired
    private CassandraUserDAO dao;

    /**
     * Test creation and retrieving of user.
   *
   */
  @Test
  public void testCreateAndRetrieve() throws Exception {
    User gU = new User("test", "test");
    service.createUser(gU);
    User gURet = service.getUser("test");
    assertEquals(gU.getUsername(), gURet.getUsername());
      assertEquals(gU.getPassword(), gURet.getPassword());
      assertThrows(UserExistsException.class, () -> service.createUser(gU));
  }

    /**
     * Test UserDoesNotExistException
     */
    @Test
    public void testUserDoesNotExist() {
        assertThrows(UserDoesNotExistException.class, () -> service.getUser("test2"));
    }

    @Test
    void shouldThrowExceptionInCaseOfUpdatingNonExistingUser() {
        assertThrows(UserDoesNotExistException.class, () -> service.updateUser(new SpringUser("user1", "password1")));
    }

    @Test
    void shouldCorrectlyUpdateUser() throws Exception {
        service.createUser(new SpringUser("test3", "test3"));
        service.updateUser(new SpringUser("test3", "test4"));
        User user = service.getUser("test3");
        assertEquals("test4", user.getPassword());
    }

  @Test
  void shouldLockTheUser() throws Exception {
      service.createUser(new SpringUser("user1", "user1"));
      User user = service.getUser("user1");
      assertFalse(user.isLocked());
      service.lockUser("user1");
      user = service.getUser("user1");
      assertTrue(user.isLocked());
  }

    @Test
    void shouldUnlockTheUser() throws Exception {
        service.createUser(new SpringUser("user1", "user1", Collections.emptySet(), true));
        User user = service.getUser("user1");
        assertTrue(user.isLocked());
        service.unlockUser("user1");
        user = service.getUser("user1");
        assertFalse(user.isLocked());
    }

    @Test
    void shouldThrowExceptionInCaseOfLockingNonExistingUser() {
        assertThrows(UserDoesNotExistException.class, () -> service.lockUser("nonExistingUser"));
    }

    @Test
    void shouldThrowExceptionInCaseOfUnLockingNonExistingUser() {
        assertThrows(UserDoesNotExistException.class, () -> service.unlockUser("nonExistingUser"));
    }

}
