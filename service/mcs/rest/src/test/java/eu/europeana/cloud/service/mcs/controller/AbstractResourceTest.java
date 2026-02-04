package eu.europeana.cloud.service.mcs.controller;


import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.BasicResourceTestContext;
import eu.europeana.cloud.service.mcs.utils.testcontexts.PropertyBeansContext;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Collections;

import static org.mockito.Mockito.when;

@WebAppConfiguration
@ContextConfiguration(classes = {ServiceConfiguration.class, PropertyBeansContext.class,
        UnifiedExceptionsMapper.class, BasicResourceTestContext.class})
@WebMvcTest(properties = "spring.main.allow-bean-definition-overriding=true")
@TestPropertySource("classpath:mcs-test.properties")
public abstract class AbstractResourceTest {

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

  @BeforeEach
  public void prepareMockMvc() {
    SecurityContextHolder.getContext().setAuthentication(null);
    mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                             .build();
  }

}
