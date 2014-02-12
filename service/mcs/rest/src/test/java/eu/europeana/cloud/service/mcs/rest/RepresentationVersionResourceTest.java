package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.CannotModifyPersistentRepresentationExceptionMapper;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.VersionNotExistsExceptionMapper;

import java.util.Arrays;
import java.util.Date;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import static junitparams.JUnitParamsRunner.$;
import junitparams.Parameters;

import org.glassfish.jersey.client.ClientProperties;
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
public class RepresentationVersionResourceTest extends JerseyTest {

    private RecordService recordService;

    static final private String globalId = "1";
    static final private String schema = "DC";
    static final private String version = "1.0";
    static final private String fileName = "1.xml";
    static final private String persistPath = URITools.getPath(RepresentationVersionResource.class,
        "persistRepresentation", globalId, schema, version).toString();
    static final private String copyPath = URITools.getPath(RepresentationVersionResource.class, "copyRepresentation",
        globalId, schema, version).toString();

    static final private Representation representation = new Representation(globalId, schema, version, null, null,
            "DLF", Arrays.asList(new File(fileName, "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01",
                    12345, null)), true, new Date());


    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RepresentationVersionResource.class)
                .registerClasses(RecordNotExistsExceptionMapper.class)
                .registerClasses(RepresentationNotExistsExceptionMapper.class)
                .registerClasses(VersionNotExistsExceptionMapper.class)
                .registerClasses(CannotModifyPersistentRepresentationExceptionMapper.class)
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
    public void testGetRepresentationVersion(MediaType mediaType)
            throws Exception {
        Representation expected = new Representation(representation);
        URITools.enrich(expected, getBaseUri());
        when(recordService.getRepresentation(globalId, schema, version)).thenReturn(new Representation(representation));

        Response response = target(URITools.getVersionPath(globalId, schema, version).toString()).request(mediaType)
                .get();

        assertThat(response.getStatus(), is(200));
        assertThat(response.getMediaType(), is(mediaType));
        Representation entity = response.readEntity(Representation.class);
        assertThat(entity, is(expected));
        verify(recordService, times(1)).getRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    @Parameters(method = "mimeTypes")
    public void testGetLatestRepresentationVersion(MediaType mediaType)
            throws Exception {
        when(recordService.getRepresentation(globalId, schema)).thenReturn(new Representation(representation));

        client().property(ClientProperties.FOLLOW_REDIRECTS, false);
        Response response = target(
            URITools.getVersionPath(globalId, schema, ParamConstants.LATEST_VERSION_KEYWORD).toString()).request(
            mediaType).get();

        assertThat(response.getStatus(), is(307));
        assertThat(response.getLocation(), is(URITools.getVersionUri(getBaseUri(), globalId, schema, version)));
        verify(recordService, times(1)).getRepresentation(globalId, schema);
        verifyNoMoreInteractions(recordService);
    }


    private Object[] errors() {
        return $($(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(), 404));
    }


    @Test
    @Parameters(method = "errors")
    public void testGetRepresentationVersionReturns404IfRepresentationOrRecordOrVersionDoesNotExists(
            Throwable exception, String errorCode, int statusCode)
            throws Exception {
        when(recordService.getRepresentation(globalId, schema, version)).thenThrow(exception);

        Response response = target().path(URITools.getVersionPath(globalId, schema, version).toString())
                .request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(statusCode));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).getRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void testGetRepresentationVersionReturns406ForUnsupportedFormat() {
        Response response = target().path(URITools.getVersionPath(globalId, schema, version).toString())
                .request(MediaType.APPLICATION_SVG_XML_TYPE).get();

        assertThat(response.getStatus(), is(406));
    }


    @Test
    public void testDeleteRepresentation()
            throws Exception {
        Response response = target().path(URITools.getVersionPath(globalId, schema, version).toString()).request()
                .delete();

        assertThat(response.getStatus(), is(204));
        verify(recordService, times(1)).deleteRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    @Parameters(method = "errors")
    public void testDeleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
            String errorCode, int statusCode)
            throws Exception {
        Mockito.doThrow(exception).when(recordService).deleteRepresentation(globalId, schema, version);

        Response response = target().path(URITools.getVersionPath(globalId, schema, version).toString()).request()
                .delete();

        assertThat(response.getStatus(), is(statusCode));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).deleteRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void testPersistRepresentation()
            throws Exception {
        when(recordService.persistRepresentation(globalId, schema, version)).thenReturn(
            new Representation(representation));

        Response response = target(persistPath).request().post(
            Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        assertThat(response.getStatus(), is(201));
        assertThat(response.getLocation(), is(URITools.getVersionUri(getBaseUri(), globalId, schema, version)));
        verify(recordService, times(1)).persistRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    @Parameters(method = "persistErrors")
    public void testPersistRepresentationReturns40XIfExceptionOccur(Throwable exception, String errorCode,
            int statusCode)
            throws Exception {
        when(recordService.persistRepresentation(globalId, schema, version)).thenThrow(exception);

        Response response = target().path(persistPath).request()
                .post(Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        assertThat(response.getStatus(), is(statusCode));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).persistRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @SuppressWarnings("unused")
    private Object[] persistErrors() {
        return $(
            $(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(), 404),
            $(new CannotModifyPersistentRepresentationException(),
                McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString(), 405));
    }


    @Test
    public void testCopyRepresentation()
            throws Exception {
        when(recordService.copyRepresentation(globalId, schema, version))
                .thenReturn(new Representation(representation));

        Response response = target(copyPath).request().post(
            Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        assertThat(response.getStatus(), is(201));
        assertThat(response.getLocation(), is(URITools.getVersionUri(getBaseUri(), globalId, schema, version)));
        verify(recordService, times(1)).copyRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    @Parameters(method = "errors")
    public void testCopyRepresentationReturns404IfRepresentationOrRecordOrVersionDoesNotExists(Throwable exception,
            String errorCode, int statusCode)
            throws Exception {
        when(recordService.copyRepresentation(globalId, schema, version)).thenThrow(exception);

        Response response = target().path(copyPath).request()
                .post(Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        assertThat(response.getStatus(), is(statusCode));
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertThat(errorInfo.getErrorCode(), is(errorCode));
        verify(recordService, times(1)).copyRepresentation(globalId, schema, version);
        verifyNoMoreInteractions(recordService);
    }

}
