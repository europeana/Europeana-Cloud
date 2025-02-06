package eu.europeana.cloud.service.mcs.controller;


import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.BasicResourceTestContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.PropertyBeansContext;
import java.util.Collections;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@WebAppConfiguration
@ContextConfiguration(classes = {ServiceConfiguration.class, PropertyBeansContext.class,
    UnifiedExceptionsMapper.class, BasicResourceTestContext.class})
@WebMvcTest
@TestPropertySource("classpath:mcs-test.properties")
public abstract class AbstractResourceTest {

  @Rule
  public SpringClassRule springRule = new SpringClassRule();

  @Rule
  public SpringMethodRule methodRule = new SpringMethodRule();

  @Autowired
  protected WebApplicationContext applicationContext;

  protected MockMvc mockMvc;

  public static HttpServletRequest mockHttpServletRequest() {
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:8080"));
    when(request.getHeaderNames()).thenReturn(Collections.enumeration(Collections.emptyList()));
    when(request.getRequestURI()).thenReturn("http://127.0.0.1:8080/mcs/data-providers/xxx/records/xxx/representations/xxxx/filename");
    return request;
  }

  @Before
  public void prepareMockMvc() {
    SecurityContextHolder.getContext().setAuthentication(null);
    mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                             .build();
  }

}
