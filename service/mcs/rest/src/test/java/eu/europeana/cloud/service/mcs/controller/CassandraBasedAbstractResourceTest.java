package eu.europeana.cloud.service.mcs.controller;


import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.service.mcs.utils.testcontexts.CassandraBasedTestContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.PropertyBeansContext;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@WebAppConfiguration
@ContextConfiguration(
        classes = {ServiceConfiguration.class, UnifiedExceptionsMapper.class,
                CassandraBasedTestContext.class, PropertyBeansContext.class})
@WebMvcTest(properties = "spring.main.allow-bean-definition-overriding=true")
@TestPropertySource("classpath:mcs-test.properties")
public abstract class CassandraBasedAbstractResourceTest {

  @Autowired
  protected WebApplicationContext applicationContext;

  @Autowired
  protected PermissionEvaluator permissionEvaluator;

  protected MockMvc mockMvc;

  @BeforeEach
  public void prepareMockMvc() {
    SecurityContextHolder.getContext().setAuthentication(null);
    mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                             .build();
  }
}