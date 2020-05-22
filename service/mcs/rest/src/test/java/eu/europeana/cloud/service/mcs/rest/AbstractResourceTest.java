package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.service.mcs.MCSAppInitializer;
import eu.europeana.cloud.service.mcs.config.UnitedExceptionMapper;
import eu.europeana.cloud.service.mcs.utils.testcontexts.AbstractResourceTestContext;
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
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Mockito.when;

@WebAppConfiguration
@ContextConfiguration(classes = {
        MCSAppInitializer.class,
        TestServiceConfiguration.class, UnitedExceptionMapper.class,
        AbstractResourceTestContext.class, RecordsResource.class})
public class AbstractResourceTest {

    @Rule
    public SpringClassRule springRule = new SpringClassRule();

    @Rule
    public SpringMethodRule methodRule = new SpringMethodRule();

    @Autowired
    protected WebApplicationContext applicationContext;

    protected MockMvc mockMvc;

    public static HttpServletRequest mockHttpServletRequest() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        when(request.getScheme()).thenReturn("http");
        when(request.getServerName()).thenReturn("localhost");
        when(request.getServerPort()).thenReturn(80);
        return request;

        //            UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
//
//            ////       Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
//            Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
//            Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
//            Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
//            ////       Mockito.doReturn(new URI("")).when(URI_INFO).resolve((URI) Mockito.anyObject());

    }

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
