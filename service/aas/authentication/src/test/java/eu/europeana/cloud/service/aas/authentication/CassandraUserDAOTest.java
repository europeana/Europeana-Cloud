package eu.europeana.cloud.service.aas.authentication;

import static org.junit.Assert.assertTrue;

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
import org.junit.After;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/default-context.xml" })
public class CassandraUserDAOTest extends CassandraTestBase {

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

    private boolean isAdmin(final SpringUser u) {
	return u.getAuthorities().contains(
		new SimpleGrantedAuthority("ROLE_ADMIN"));
    }
}
