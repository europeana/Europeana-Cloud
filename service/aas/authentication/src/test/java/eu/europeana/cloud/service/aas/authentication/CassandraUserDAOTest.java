package eu.europeana.cloud.service.aas.authentication;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import eu.europeana.cloud.common.model.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;

import java.util.Set;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/default-context.xml"})
public class CassandraUserDAOTest extends CassandraTestBase {
    private static final String ROLE_USER = "ROLE_USER";
    private static final String ROLE_ADMIN = "ROLE_ADMIN";
    private static final Set<String> DEFAULT_USER_ROLES = ImmutableSet
            .of(ROLE_USER);



    @Autowired
    private CassandraConnectionProvider provider;

    @Autowired
    private CassandraUserDAO dao;

    /**
     * Prepare the unit tests
     */
    @Before
    public void prepare() {
        createKeyspaces();
        @SuppressWarnings("resource")
        ApplicationContext context = new ClassPathXmlApplicationContext(
                "default-context.xml");

        provider = (CassandraConnectionProvider) context.getBean("provider");
        dao = (CassandraUserDAO) context.getBean("dao");

    }

    @After
    public void clean() {
        dropAllKeyspaces();
    }

    @Test
    public void testUserWithRoles() throws Exception {

        SpringUser robinVanPersie = dao.getUser("Robin_Van_Persie");
        assertTrue(!isAdmin(robinVanPersie));

        SpringUser admin = dao.getUser("admin");
        assertTrue(isAdmin(admin));
    }

    @Test
    public void createUserTest() throws Exception {
        //given
        final String password = "PassFrank";
        final String username = "Frank";
        //when
        dao.createUser(new User(username, password, DEFAULT_USER_ROLES));
        //then
        SpringUser user = dao.getUser(username);
        assertThat(isUser(user),is(true));
        assertUser(password, username, user);
    }

    private void assertUser(String password, String username, SpringUser user) {
        assertThat(user.getUsername(),is(username));
        assertThat(user.getPassword(),is(password));
    }

    private boolean isAdmin(final SpringUser u) {
        return u.getAuthorities().contains(
                new SimpleGrantedAuthority(ROLE_ADMIN));
    }

    private boolean isUser(final SpringUser u) {
        return u.getAuthorities().contains(
                new SimpleGrantedAuthority(ROLE_USER));
    }
}
