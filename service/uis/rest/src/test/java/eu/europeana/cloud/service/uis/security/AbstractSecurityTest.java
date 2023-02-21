package eu.europeana.cloud.service.uis.security;

import org.junit.After;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;


@ContextConfiguration(locations = {
    "classpath:uis-security-business-context-test.xml",
    "classpath:authentication-context-test.xml", // authentication uses a static InMemory list of usernames, passwords
    "classpath:authorization-context-test.xml" // authorization uses Embedded cassandra
})
public abstract class AbstractSecurityTest extends CassandraTestBase {

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
}
