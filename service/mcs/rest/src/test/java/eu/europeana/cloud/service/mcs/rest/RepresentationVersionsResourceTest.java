package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import junitparams.JUnitParamsRunner;
import static junitparams.JUnitParamsRunner.$;
import junitparams.Parameters;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.springframework.context.ApplicationContext;

@RunWith(JUnitParamsRunner.class)
public class RepresentationVersionsResourceTest extends JerseyTest {

    private RecordService recordService;

    static final private String GLOBAL_ID = "1";
    static final private String SCHEMA = "DC";
    static final private String VERSION = "1.0";

    private static final String LIST_VERSIONS_PATH = URITools.getListVersionsPath(GLOBAL_ID, SCHEMA).toString();
    static final private List<Representation> REPRESENTATIONS = ImmutableList.of(new Representation(GLOBAL_ID, SCHEMA,
            VERSION, null, null, "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
                    "2013-01-01", 12345, null)), true, new Date()));


    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RepresentationVersionsResource.class)
                .registerClasses(RecordNotExistsExceptionMapper.class)
                .registerClasses(RepresentationNotExistsExceptionMapper.class)
                .property("contextConfigLocation", "classpath:testContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        Mockito.reset(recordService);
    }


    @SuppressWarnings("unused")
    private Object[] mimeTypes() {
        return $($(MediaType.APPLICATION_XML_TYPE), $(MediaType.APPLICATION_JSON_TYPE));
    }


    @Test
    @Parameters(method = "mimeTypes")
    public void testListVersions(MediaType mediaType)
            throws Exception {
        List<Representation> expected = copy(REPRESENTATIONS);
        Representation expectedRepresentation = expected.get(0);
        URITools.enrich(expectedRepresentation, getBaseUri());
        when(recordService.listRepresentationVersions(GLOBAL_ID, SCHEMA)).thenReturn(copy(REPRESENTATIONS));

        Response response = target(LIST_VERSIONS_PATH).request(mediaType).get();

        assertThat(response.getStatus(), is(200));
        assertThat(response.getMediaType(), is(mediaType));
        List<Representation> entity = response.readEntity(new GenericType<List<Representation>>() {
        });
        assertThat(entity, is(expected));
        verify(recordService, times(1)).listRepresentationVersions(GLOBAL_ID, SCHEMA);
        verifyNoMoreInteractions(recordService);
    }


    private List<Representation> copy(List<Representation> representations) {
        List<Representation> expected = new ArrayList<>();
        for (Representation representation : representations) {
            expected.add(new Representation(representation));
        }
        return expected;
    }


    @SuppressWarnings("unused")
    private Object[] errors() {
        return $($(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()));
    }


    @Test
    @Parameters(method = "errors")
    public void testListVersionsReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception, String errorCode)
            throws Exception {
        when(recordService.listRepresentationVersions(GLOBAL_ID, SCHEMA)).thenThrow(exception);

        Response response = target().path(LIST_VERSIONS_PATH).request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(404));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).listRepresentationVersions(GLOBAL_ID, SCHEMA);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void testListVersionsReturns406ForUnsupportedFormat() {
        Response response = target().path(LIST_VERSIONS_PATH).request(MediaType.APPLICATION_SVG_XML_TYPE).get();

        assertThat(response.getStatus(), is(406));
    }

}
