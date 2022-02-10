package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.service.mcs.MCSAppInitializer;
import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import eu.europeana.cloud.service.mcs.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.BasicResourceTestContext;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.http.HttpServletRequest;

import java.util.Collections;

import static org.mockito.Mockito.when;

@WebAppConfiguration
@ContextConfiguration(classes = {MCSAppInitializer.class, ServiceConfiguration.class,
        UnifiedExceptionsMapper.class, BasicResourceTestContext.class})
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
        return request;
    }

    @Before
    public void prepareMockMvc() {
        mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                .build();
    }

}
