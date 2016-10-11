package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RevisionNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(JUnitParamsRunner.class)
public class RepresentationRevisionsResourceTest extends JerseyTest {
    
    private RecordService recordService;
    
    static final private String globalId = "1";
    static final private String schema = "DC";
    static final private String revisionProviderId = "ABC";
    static final private String revisionId = "rev1";
    static final private String version = "1.0";
    static final private Date revisionTimestamp = new Date();
    static final private RepresentationRevisionResponse representationResponse = new RepresentationRevisionResponse(globalId, schema, version, Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
                            null)),"ABC_rev1", revisionTimestamp);

    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RepresentationRevisionsResource.class)
                .registerClasses(RevisionNotExistsExceptionMapper.class)
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
    public void getRepresentationRevisionResponse(MediaType mediaType)
            throws Exception {
        RepresentationRevisionResponse expected = new RepresentationRevisionResponse(representationResponse);
        ArrayList<File> files = new ArrayList<>();
        files.add(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
                "2013-01-01", 12345L, URI.create("http://localhost:9998/records/" + globalId
                + "/representations/" + schema + "/versions/" + version + "/files/1.xml")));
        expected.setFiles(files);

        when(recordService.getRepresentationRevision(globalId,
                schema, RevisionUtils.getRevisionKey(revisionProviderId, revisionId), revisionTimestamp)).thenReturn(new RepresentationRevisionResponse(representationResponse));
        
        Response response = target(URITools.getRepresentationRevisionsPath(globalId, schema, revisionId).toString()).queryParam(ParamConstants.REVISION_PROVIDER_ID, revisionProviderId).request(mediaType)
                .get();
        
        assertThat(response.getStatus(), is(200));
        assertThat(response.getMediaType(), is(mediaType));
        RepresentationRevisionResponse entity = response.readEntity(RepresentationRevisionResponse.class);
        assertThat(entity, is(expected));
        verify(recordService, times(1)).getRepresentationRevision(globalId, schema, RevisionUtils.getRevisionKey(revisionProviderId, revisionId), revisionTimestamp);
        verifyNoMoreInteractions(recordService);
    }
    
    @Test
    public void getRepresentationReturns406ForUnsupportedFormat() {
        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionId).toString()).queryParam(ParamConstants.REVISION_PROVIDER_ID, revisionProviderId)
                .request(MediaType.APPLICATION_SVG_XML_TYPE).get();
        
        assertThat(response.getStatus(), is(406));
    }
    

    @SuppressWarnings("unused")
    private Object[] representationRevisionErrors() {
        return $($(new RevisionNotExistsException(), McsErrorCode.REVISION_NOT_EXISTS.toString()));
    }
    
    @Test
    @Parameters(method = "representationRevisionErrors")
    public void getRepresentationRevisionsReturns404IfRevisionDoesNotExists(Throwable exception, String errorCode)
            throws Exception {
        when(recordService.getRepresentationRevision(globalId, schema, RevisionUtils.getRevisionKey(revisionProviderId, revisionId), revisionTimestamp)).thenThrow(exception);
        
        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionId).toString()).queryParam(ParamConstants.REVISION_PROVIDER_ID, revisionProviderId)
                .request(MediaType.APPLICATION_XML).get();
        
        assertThat(response.getStatus(), is(404));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).getRepresentationRevision(globalId, schema, RevisionUtils.getRevisionKey(revisionProviderId, revisionId), revisionTimestamp);
        verifyNoMoreInteractions(recordService);
    }
}
