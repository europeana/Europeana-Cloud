package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class UncauchtExceptionMapperTest extends JerseyTest {

    private RecordService recordService;


    @Override
    public Application configure() {
        return null; //new JerseyConfig().property("contextConfigLocation", "classpath:testContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        Mockito.reset(recordService);
    }


    @Test
    public void shouldReturnErrorInfoOnEveryException()
            throws Exception {
        Throwable exception = new RuntimeException("error details");
        when(recordService.getRecord(Matchers.anyString())).thenThrow(exception);

        Response response = target().path(URITools.getRepresentationsPath("id").toString())
                .request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
        assertThat(errorInfo.getDetails(), is(exception.getMessage()));
    }
}
