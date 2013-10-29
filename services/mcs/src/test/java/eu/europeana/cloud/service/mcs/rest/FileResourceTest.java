package eu.europeana.cloud.service.mcs.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.ContentService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * FileResourceTest
 */
public class FileResourceTest extends JerseyTest {

    private static RecordService recordService;

    private static ContentService contentService;

    private Representation recordRepresentation;

    private File file;

    private WebTarget fileWebTarget;

    private WebTarget representationWebTarget;

    private WebTarget filesWebTarget;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        contentService = applicationContext.getBean(ContentService.class);

        recordRepresentation = recordService.createRepresentation("1", "1", "1");
        file = new File();
        file.setFileName("fileName");
        file.setMimeType("mime/fileSpecialMime");

        representationWebTarget = target("/records/{ID}/representations/{REPRESENTATION}/versions/{VERSION}")
                .resolveTemplates(ImmutableMap.<String, Object>of(
                ParamConstants.P_GID, recordRepresentation.getRecordId(),
                ParamConstants.P_REP, recordRepresentation.getSchema(),
                ParamConstants.P_VER, recordRepresentation.getVersion()));
        filesWebTarget = representationWebTarget.path("files");
        fileWebTarget = filesWebTarget.path(file.getFileName());
    }


    @After
    public void cleanUp() {
        reset(recordService);
        recordService.deleteRepresentation(recordRepresentation.getRecordId(), recordRepresentation.getSchema());
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldReturnContentWithinRange() {
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldReturnErrorWhenRangeUnreachable() {
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldRemainConsistentWithConcurrentPuts() {
    }


    @Test
    public void shouldOverrideFileOnRepeatedPut()
            throws IOException {
        byte[] content = {1, 2, 3, 4};

        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), putFileResponse.getStatus());

        byte[] contentModified = {5, 6, 7};

        multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(contentModified), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(), putFileResponse.getStatus());

        Response getFileResponse = fileWebTarget.request().get();
        assertEquals("Unexpected status code", Response.Status.OK.getStatusCode(), getFileResponse.getStatus());
        byte[] responseContent = ByteStreams.toByteArray(getFileResponse.readEntity(InputStream.class));

        assertArrayEquals("Read data is different from written", contentModified, responseContent);
    }


    @Test
    public void shouldDeleteFile() {
        final byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), putFileResponse.getStatus());

        Response deleteFileResponse = fileWebTarget.request().delete();
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(), deleteFileResponse.getStatus());

        Response getFileResponse = fileWebTarget.request().get();
        assertEquals("Unexpected status code", Response.Status.NOT_FOUND.getStatusCode(), getFileResponse.getStatus());

        ErrorInfo deleteErrorInfo = getFileResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());

        Response getRepresentationResponse = representationWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getRepresentationResponse.getStatus());
        Representation rep = getRepresentationResponse.readEntity(Representation.class);

        if (rep.getFiles() != null) {
            for (File f : rep.getFiles()) {
                assertNotSame("File was deleted but is still in representation's files", f.getFileName(), file.getFileName());
            }
        }
    }


    @Test
    public void shouldUploadDataWithPut()
            throws IOException {
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), putFileResponse.getStatus());
        assertEquals("Unexpected content location", fileWebTarget.getUri(), putFileResponse.getLocation());
        assertEquals("File content tag mismatch", contentMd5, putFileResponse.getEntityTag().getValue());

        Response getFileResponse = fileWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getFileResponse.getStatus());

        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        assertArrayEquals("Read data is different from written", content, responseContent);
        assertEquals("File content tag mismatch", contentMd5, getFileResponse.getEntityTag().getValue());

        Response getRepresentationResponse = representationWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getRepresentationResponse.getStatus());
        Representation rep = getRepresentationResponse.readEntity(Representation.class);
        File insertedFile = null;
        for (File f : rep.getFiles()) {
            if (f.getFileName().equals(file.getFileName())) {
                insertedFile = f;
                break;
            }
        }
        assertNotNull("Cannot find inserted file in representation's files", insertedFile);
    }


    @Test
    public void shouldUploadDataWithPost()
            throws IOException {
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response postFileResponse = filesWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), postFileResponse.getStatus());
        assertEquals("File content tag mismatch", contentMd5, postFileResponse.getEntityTag().getValue());

        URI putFileLocation = postFileResponse.getLocation();
        assertNotNull(putFileLocation);
        fileWebTarget = client().target(putFileLocation);

        Response getFileResponse = fileWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getFileResponse.getStatus());

        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        assertArrayEquals("Read data is different from written", content, responseContent);
        assertEquals("File content tag mismatch", contentMd5, getFileResponse.getEntityTag().getValue());

        Response getRepresentationResponse = representationWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getRepresentationResponse.getStatus());
        Representation rep = getRepresentationResponse.readEntity(Representation.class);

        assertTrue("Representation does not have inserted file", rep.getFiles() != null && rep.getFiles().size() == 1);
    }
}
