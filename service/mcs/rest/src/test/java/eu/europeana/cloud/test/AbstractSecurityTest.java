package eu.europeana.cloud.test;

import static eu.europeana.cloud.service.mcs.controller.AbstractResourceTest.mockHttpServletRequest;

import eu.europeana.cloud.service.mcs.config.AuthorizationConfiguration;
import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.PropertyBeansContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.SecurityTestContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.TestAuthentificationConfiguration;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;


/**
 * Helper class thats logs-in people to perform permission tests.
 */
//@ContextConfiguration(locations = {
//        "classpath:authentication-context-test.xml", // authentication uses a static InMemory list of usernames, passwords
//        "classpath:authorization-context-test.xml", // authorization uses Embedded cassandra
//		"classpath:aaTestContext.xml"
//        })
@RunWith(CassandraTestRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {
    TestAuthentificationConfiguration.class,
    PropertyBeansContext.class,
    ServiceConfiguration.class,
    AuthorizationConfiguration.class,
    UnifiedExceptionsMapper.class,
    SecurityTestContext.class,
        })
@TestPropertySource("classpath:mcs-test.properties")
@EnableWebSecurity
@EnableMethodSecurity
public abstract class AbstractSecurityTest {

  @Rule
  public SpringClassRule springRule = new SpringClassRule();

  @Rule
  public SpringMethodRule methodRule = new SpringMethodRule();

  @Autowired
  protected WebApplicationContext applicationContext;

  protected MockMvc mockMvc;

  protected HttpServletRequest URI_INFO;

  /****/

  @Before
  public void prepareMockMvc() {
    mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext).build();

    URI_INFO = mockHttpServletRequest();
  }


  protected String getBaseUri() {
    return "localhost:80/";
  }


  @Autowired
  private AuthenticationManager authenticationManager;

  @Before
  public synchronized void clear() {
    SecurityContextHolder.clearContext();
  }

  protected synchronized void login(String name, String password) {
    Authentication auth = new UsernamePasswordAuthenticationToken(name, password);
    SecurityContextHolder.getContext().setAuthentication(authenticationManager.authenticate(auth));
  }

  protected synchronized void logoutEveryone() {
    SecurityContextHolder.getContext().setAuthentication(null);
  }
}
