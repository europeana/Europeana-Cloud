package eu.europeana.cloud.service.mcs.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.ws.rs.Path;
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
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * FileResourceTest
 */
public class FileResourceTest extends JerseyTest {

    private static RecordService recordService;

    private Representation rep;

    private File file;

    private WebTarget fileWebTarget;

    private WebTarget representationWebTarget;

    private WebTarget filesWebTarget;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);

        rep = recordService.createRepresentation("1", "1", "1");
        file = new File();
        file.setFileName("fileName");
        file.setMimeType("mime/fileSpecialMime");

        Map<String, Object> allPathParams = ImmutableMap.<String, Object>of(
                ParamConstants.P_GID, rep.getRecordId(),
                ParamConstants.P_REP, rep.getSchema(),
                ParamConstants.P_VER, rep.getVersion(),
                ParamConstants.P_FILE, file.getFileName());
        representationWebTarget = target(RepresentationVersionResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
        filesWebTarget = target(FilesResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
        fileWebTarget = target(FileResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
    }


    @After
    public void cleanUp() {
        recordService.deleteRepresentation(rep.getRecordId(), rep.getSchema());
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
    @Ignore(value = "TODO: implement")
    public void shouldReturnErrorOnHashMismatch() {
    }


    @Test
    public void shouldOverrideFileOnRepeatedPut()
            throws IOException {
        // given particular content in service
        byte[] content = {1, 2, 3, 4};
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(content));

        // when you override it with another content
        byte[] contentModified = {5, 6, 7};
        String contentModifiedMd5 = Hashing.md5().hashBytes(contentModified).toString();

        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(contentModified), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(), putFileResponse.getStatus());

        // then the content in service should be also modivied
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String retrievedFileMd5 = recordService.getContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file.getFileName(), baos);
        assertArrayEquals("Read data is different from written", contentModified, baos.toByteArray());
        assertEquals("MD5 checksum is different than written", contentModifiedMd5, retrievedFileMd5);
    }


    @Test
    public void shouldDeleteFile()
            throws IOException {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(content));

        Response deleteFileResponse = fileWebTarget.request().delete();
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(), deleteFileResponse.getStatus());

        Representation representation = recordService.getRepresentation(rep.getRecordId(), rep.getSchema(), rep.getVersion());
        assertTrue(representation.getFiles().isEmpty());
    }


    @Test
    public void shouldReturn404WhenDeletingNonExistingFile() {
        Response deleteFileResponse = fileWebTarget.request().delete();
        assertEquals("Unexpected status code", Response.Status.NOT_FOUND.getStatusCode(), deleteFileResponse.getStatus());
        ErrorInfo deleteErrorInfo = deleteFileResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());
    }


    @Test
    public void shouldUploadDataWithPut()
            throws IOException {
        // given particular (random in this case) content
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        // when content is added to record representation
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content), MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), putFileResponse.getStatus());
        assertEquals("Unexpected content location", fileWebTarget.getUri(), putFileResponse.getLocation());
        assertEquals("File content tag mismatch", contentMd5, putFileResponse.getEntityTag().getValue());

        // then data should be in record service
        rep = recordService.getRepresentation(rep.getRecordId(), rep.getSchema(), rep.getVersion());
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        recordService.getContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file.getFileName(), contentBos);
        assertEquals(1, rep.getFiles().size());

        File insertedFile = rep.getFiles().get(0);
        assertEquals("MD5 file mismatch", contentMd5, insertedFile.getMd5());
        assertEquals(content.length, insertedFile.getContentLength());
        assertArrayEquals(content, contentBos.toByteArray());
    }


    @Test
    public void shouldRetrieveContent()
            throws IOException {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(content));

        // when this file is requested
        Response getFileResponse = fileWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getFileResponse.getStatus());

        // then concent should be equal to the previously put
        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        assertArrayEquals("Read data is different from written", content, responseContent);
        assertEquals("File content tag mismatch", contentMd5, getFileResponse.getEntityTag().getValue());
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
