package eu.europeana.cloud.service.uis.security;

import eu.europeana.cloud.service.uis.config.AuthenticationTestContext;
import eu.europeana.cloud.service.uis.config.AuthorizationTestContext;
import eu.europeana.cloud.service.uis.config.SecurityTestContext;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;


@ContextConfiguration(classes = {
        AuthenticationTestContext.class,
        AuthorizationTestContext.class,
        SecurityTestContext.class
})
public abstract class AbstractSecurityTest extends CassandraTestBase {

  @Autowired
  @Qualifier("authenticationManager")
  private AuthenticationManager authenticationManager;

  @AfterEach
  void clear() {
    SecurityContextHolder.clearContext();
  }

  protected void login(String name, String password) {
    Authentication auth = new UsernamePasswordAuthenticationToken(name, password);
    SecurityContextHolder.getContext().setAuthentication(authenticationManager.authenticate(auth));
  }
}
