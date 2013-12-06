package eu.europeana.cloud.service.mcs.rest;

import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import java.net.URI;
import java.util.Collections;
import java.util.Date;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Lists;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;

@RunWith(JUnitParamsRunner.class)
public class RecordsResourceTest extends JerseyTest {

    private RecordService recordService;


    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RecordsResource.class)
                .registerClasses(RecordNotExistsExceptionMapper.class)
                .property("contextConfigLocation", "classpath:testContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        Mockito.reset(recordService);
    }


    @Test
    @Parameters(method = "mimeTypes")
    public void getRecord(MediaType mediaType) {
        String globalId = "global1";
        Record record = new Record(globalId, Lists.newArrayList(new Representation(globalId, "DC", "1", null, null,
                "FBC", Lists.newArrayList(new File("dc.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
                        "2013-10-21", 12345L, URI.create("http://examplecloud.eu/records/" + globalId
                                + "/representations/DC/versions/1/dc.xml"))), false, new Date())));
        Record expected = new Record(record);
        Representation expectedRepresentation = expected.getRepresentations().get(0);
        // prepare expected representation: 
        // - erase record id and files
        // - set URIs
        expectedRepresentation.setRecordId(null);
        expectedRepresentation.setFiles(Collections.<File> emptyList());
        expectedRepresentation.setAllVersionsUri(URI.create(getBaseUri() + "records/" + globalId
                + "/representations/DC/versions"));
        expectedRepresentation.setUri(URI.create(getBaseUri() + "records/" + globalId
                + "/representations/DC/versions/1"));
        when(recordService.getRecord(globalId)).thenReturn(record);

        Response response = target().path("/records/" + globalId).request(mediaType).get();

        assertThat(response.getStatus(), is(200));
        assertThat(response.getMediaType(), is(mediaType));
        Object entity = response.readEntity(Record.class);
        assertThat((Record) entity, is(expected));
        verify(recordService, times(1)).getRecord(globalId);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void getRecordReturns406ForUnsupportedFormat() {
        Response response = target().path("/records/global1").request(MediaType.APPLICATION_SVG_XML_TYPE).get();

        assertThat(response.getStatus(), is(406));
    }


    @SuppressWarnings("unused")
    private Object[] mimeTypes() {
        return $($(MediaType.APPLICATION_XML_TYPE), $(MediaType.APPLICATION_JSON_TYPE));
    }


    @Test
    public void getRecordReturns404IfRecordDoesNotExists() {
        String globalId = "global1";
        Throwable exception = new RecordNotExistsException();
        when(recordService.getRecord(globalId)).thenThrow(exception);

        Response response = target().path("/records/" + globalId).request(MediaType.APPLICATION_XML).get();

        assertThat(response.getStatus(), is(404));
        verify(recordService, times(1)).getRecord(globalId);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void deleteRecord() {
        String globalId = "global1";

        Response response = target().path("/records/" + globalId).request().delete();

        assertThat(response.getStatus(), is(204));
        verify(recordService, times(1)).deleteRecord(globalId);
        verifyNoMoreInteractions(recordService);
    }


    @Test
    public void deleteRecordReturns404IfRecordDoesNotExists()
            throws Exception {
        String globalId = "global1";
        Throwable exception = new RecordNotExistsException();
        Mockito.doThrow(exception).when(recordService).deleteRecord(globalId);

        Response response = target().path("/records/" + globalId).request(MediaType.APPLICATION_XML).delete();

        assertThat(response.getStatus(), is(404));
        verify(recordService, times(1)).deleteRecord(globalId);
        verifyNoMoreInteractions(recordService);
    }
}
