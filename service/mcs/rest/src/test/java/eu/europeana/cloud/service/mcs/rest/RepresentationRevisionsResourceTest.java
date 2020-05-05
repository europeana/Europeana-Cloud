package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.core.Authentication;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
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
    static final private String revisionName = "rev1";
    static final private String version = "1.0";
    static final private Date revisionTimestamp = new Date();
    static final private RepresentationRevisionResponse representationResponse = new RepresentationRevisionResponse(globalId, schema, version, Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
            null)), revisionProviderId, revisionName, revisionTimestamp);

    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RepresentationRevisionsResource.class)
//                .registerClasses(RevisionNotExistsExceptionMapper.class)
                .property("contextConfigLocation", "classpath:testContext.xml");
    }

    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        Mockito.reset(recordService);
        //
        AclPermissionEvaluator permissionEvaluator = applicationContext.getBean(AclPermissionEvaluator.class);
        Mockito.when(
                permissionEvaluator.hasPermission(
                        Mockito.any(Authentication.class),
                        Mockito.any(Serializable.class),
                        Mockito.any(String.class),
                        Mockito.anyObject()))
                .thenReturn(true);
    }

    @SuppressWarnings("unused")
    private Object[] mimeTypes() {
        return $($(MediaType.APPLICATION_XML_TYPE), $(MediaType.APPLICATION_JSON_TYPE));
    }

    @Test
    @Parameters(method = "mimeTypes")
    public void getRepresentationByRevisionResponse(MediaType mediaType)
            throws Exception {
        RepresentationRevisionResponse representationRevisionResponse = new RepresentationRevisionResponse(representationResponse);
        ArrayList<File> files = new ArrayList<>(1);
        files.add(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
                "2013-01-01", 12345L, URI.create("http://localhost:80/records/" + globalId
                + "/representations/" + schema + "/versions/" + version + "/files/1.xml")));
        representationRevisionResponse.setFiles(files);

        Representation representation = new Representation(representationRevisionResponse.getCloudId(), representationRevisionResponse.getRepresentationName(), representationRevisionResponse.getVersion(),
                null, null, representationRevisionResponse.getRevisionProviderId(), representationRevisionResponse.getFiles(), new ArrayList<Revision>(), false, representationRevisionResponse.getRevisionTimestamp());


        List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
        expectedResponse.add(representationRevisionResponse);

        doReturn(expectedResponse).when(recordService).getRepresentationRevisions(globalId,
                schema, revisionProviderId, revisionName, null);

        when(recordService.getRepresentation(globalId, representationResponse.getRepresentationName(), representationResponse.getVersion())).thenReturn(representation);

        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName).toString()).queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId).request(mediaType)
                .get();
        assertThat(response.getStatus(), is(200));
        assertThat(response.getMediaType(), is(mediaType));
        List<Representation> entity = response.readEntity(new GenericType<List<Representation>>() {
        });
        assertThat(entity.size(), is(1));
        assertThat(entity.get(0), is(representation));
        verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);
        verify(recordService, times(1)).getRepresentation(globalId, schema, representationRevisionResponse.getVersion());
        verifyNoMoreInteractions(recordService);
    }

    @Test
    public void getRepresentationReturns406ForUnsupportedFormat() {
        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName).toString()).queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
                .request(MediaType.APPLICATION_SVG_XML_TYPE).get();

        assertThat(response.getStatus(), is(406));
    }


    @Test
    public void getRepresentationByRevisionsThrowExceptionWhenReturnsEmptyObjectIfRevisionDoesNotExists()
            throws Exception {
        List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
        expectedResponse.add(new RepresentationRevisionResponse());

        doReturn(expectedResponse).when(recordService).getRepresentationRevisions(globalId,
                schema, revisionProviderId, revisionName, null);

        doThrow(RepresentationNotExistsException.class).when(recordService).getRepresentation(anyString(), anyString(), anyString());

        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName).toString()).queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
                .request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(500));
        verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);
        verify(recordService, times(1)).getRepresentation(anyString(), anyString(), anyString());
    }

    @Test
    public void getRepresentationByRevisionsThrowExceptionWhenReturnsRepresentationRevisionResponseIsNull()
            throws Exception {
        when(recordService.getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null)).thenReturn(null);
        Response response = target().path(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName).toString()).queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
                .request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(500));
        verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);

    }
}

