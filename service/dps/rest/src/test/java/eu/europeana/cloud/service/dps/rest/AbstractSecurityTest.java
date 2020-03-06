package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.utils.PermissionManager;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;


/**
 * Helper class thats logs-in people to perform permission tests.
 */
@ContextConfiguration(classes = {AuthentificationTestContext.class, AuthorizationTestContext.class, PermissionManager.class, AbstractSecurityTestContext.class, RecordContext.class})
@TestPropertySource(properties = {"numberOfElementsOnPage=100", "maxIdentifiersCount=100"})
public abstract class AbstractSecurityTest extends CassandraAATestRunner {

    @Autowired
    @Qualifier("authenticationManager")
    private AuthenticationManager authenticationManager;

    @After
    public void clear() {
        SecurityContextHolder.clearContext();
    }

    protected void login(String name, String password) {
        Authentication auth = new UsernamePasswordAuthenticationToken(name, password);
        SecurityContextHolder.getContext().setAuthentication(authenticationManager.authenticate(auth));
    }

    protected void logoutEveryone() {
        SecurityContextHolder.getContext().getAuthentication().setAuthenticated(false);
    }
}
