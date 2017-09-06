package eu.europeana.cloud.test;

import org.junit.After;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;


/**
 * Helper class thats logs-in people to perform permission tests.
 */
@ContextConfiguration(locations = {
        "classpath:authentication-context-test.xml", // authentication uses a static InMemory list of usernames, passwords
        "classpath:authorization-context-test.xml", // authorization uses Embedded cassandra
		"classpath:aaTestContext.xml"
        })
	public abstract class AbstractSecurityTest extends CassandraAATestRunner {

    @Autowired
    @Qualifier("authenticationManager")
    private AuthenticationManager authenticationManager;

    @Before
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
