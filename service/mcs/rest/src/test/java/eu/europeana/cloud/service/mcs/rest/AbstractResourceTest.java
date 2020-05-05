package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.service.mcs.config.UnitedExceptionMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.AbstractResourceTestContext;
import org.junit.Before;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.net.URI;
import java.net.URISyntaxException;

@WebAppConfiguration
@ContextConfiguration(classes = {TestServiceConfiguration.class, UnitedExceptionMapper.class,
        AbstractResourceTestContext.class, RecordsResource.class})
public class AbstractResourceTest {

    @Rule
    public SpringClassRule springRule = new SpringClassRule();

    @Rule
    public SpringMethodRule methodRule = new SpringMethodRule();

    @Autowired
    protected WebApplicationContext applicationContext;

    protected MockMvc mockMvc;

    @Before
    public void prepareMockMvc() throws Exception {
        mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                .build();
    }

    protected URI getBaseUri() {
        try {
            return new URI("http://localhost:80/");
        } catch (URISyntaxException e) {
            //this should never happen.
            throw new RuntimeException(e);
        }
    }

}
