package eu.europeana.cloud.service.mcs.rest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.mockito.Mockito;

/**
 * FileResourceTest
 */
@RunWith(JUnitParamsRunner.class)
public class FileResourceTest extends JerseyTest {

    private RecordService recordService;

    private Representation rep;

    private File file;

    private WebTarget fileWebTarget;

    private UISClientHandler uisHandler;


    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        DataProvider dataProvider = new DataProvider();
        dataProvider.setId("1");
        Mockito.doReturn(true).when(uisHandler).providerExistsInUIS("1");
        Mockito.doReturn(true).when(uisHandler).recordExistInUIS(Mockito.anyString());
        rep = recordService.createRepresentation("1", "1", "1");
        file = new File();
        file.setFileName("fileName");
        file.setMimeType("mime/fileSpecialMime");

        Map<String, Object> allPathParams = ImmutableMap.<String, Object> of(ParamConstants.P_GID, rep.getRecordId(),
            ParamConstants.P_SCHEMA, rep.getSchema(), ParamConstants.P_VER, rep.getVersion(), ParamConstants.P_FILE,
            file.getFileName());
        fileWebTarget = target(FileResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
    }


    @After
    public void cleanUp()
            throws Exception {
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
    public void shouldReturnContentWithinRangeOffset()
            throws Exception {
        // given particular content in service
        byte[] content = { 1, 2, 3, 4 };
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when part of file is requested (skip first byte)
        Response getFileResponse = fileWebTarget.request().header("Range", "bytes=1-").get();
        assertEquals(Response.Status.PARTIAL_CONTENT.getStatusCode(), getFileResponse.getStatus());

        // then retrieved content should consist of second and third byte of inserted byte array
        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        byte[] expectedResponseContent = copyOfRange(content, 1, content.length - 1);
        assertArrayEquals("Read data is different from requested range", expectedResponseContent, responseContent);
    }


    @Test
    @Parameters({ "1,2", "0,0", "0,1", "3,3", "0,3", "3,4" })
    public void shouldReturnContentWithinRange(Integer rangeStart, Integer rangeEnd)
            throws Exception {
        // given particular content in service
        byte[] content = { 1, 2, 3, 4 };
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when part of file is requested (2 bytes with 1 byte offset)
        Response getFileResponse = fileWebTarget.request()
                .header("Range", String.format("bytes=%d-%d", rangeStart, rangeEnd)).get();
        assertEquals(Response.Status.PARTIAL_CONTENT.getStatusCode(), getFileResponse.getStatus());

        // then retrieved content should consist of second and third byte of inserted byte array
        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        byte[] expectedResponseContent = copyOfRange(content, rangeStart, rangeEnd);
        assertArrayEquals("Read data is different from requested range", expectedResponseContent, responseContent);
    }


    /**
     * Copy the specified range of array to a new array. This method works similar to
     * {@link Arrays#copyOfRange(byte[], int, int)}, but final index is inclusive.
     * 
     * @see Arrays#copyOfRange(boolean[], int, int)
     */
    private byte[] copyOfRange(byte[] originalArray, int start, int end) {
        if (end > originalArray.length - 1) {
            end = originalArray.length - 1;
        }
        return Arrays.copyOfRange(originalArray, start, end + 1);
    }


    @Test
    public void shouldReturnErrorWhenRequestedRangeNotSatisfiable()
            throws Exception {
        // given particular content in service
        byte[] content = { 1, 2, 3, 4 };
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when unsatisfiable content range is requested
        Response getFileResponse = fileWebTarget.request().header("Range", "bytes=4-5").get();

        // then should response that requested range is not satisfiable
        assertEquals(Response.Status.REQUESTED_RANGE_NOT_SATISFIABLE.getStatusCode(), getFileResponse.getStatus());
    }


    @Test
    public void shouldReturnErrorWhenRequestedRangeNotValid()
            throws Exception {
        // given particular content in service
        byte[] content = { 1, 2, 3, 4 };
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when part of file is requested (2 bytes with 1 byte offset)
        Response getFileResponse = fileWebTarget.request().header("Range", "bytes=-2").get();

        // then should response that request is wrongly formatted
        assertEquals(Response.Status.REQUESTED_RANGE_NOT_SATISFIABLE.getStatusCode(), getFileResponse.getStatus());
    }


    @Test
    @Ignore(value = "TODO: implement")
    public void shouldReturnErrorOnHashMismatch() {
    }


    @Test
    public void shouldOverrideFileOnRepeatedPut()
            throws Exception {
        // given particular content in service
        byte[] content = { 1, 2, 3, 4 };
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when you override it with another content
        byte[] contentModified = { 5, 6, 7 };
        String contentModifiedMd5 = Hashing.md5().hashBytes(contentModified).toString();

        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(contentModified),
                    MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response putFileResponse = fileWebTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(), putFileResponse.getStatus());

        // then the content in service should be also modivied
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String retrievedFileMd5 = recordService.getContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(),
            file.getFileName(), baos);
        assertArrayEquals("Read data is different from written", contentModified, baos.toByteArray());
        assertEquals("MD5 checksum is different than written", contentModifiedMd5, retrievedFileMd5);
    }


    @Test
    public void shouldDeleteFile()
            throws Exception {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        Response deleteFileResponse = fileWebTarget.request().delete();
        assertEquals("Unexpected status code", Response.Status.NO_CONTENT.getStatusCode(),
            deleteFileResponse.getStatus());

        Representation representation = recordService.getRepresentation(rep.getRecordId(), rep.getSchema(),
            rep.getVersion());
        assertTrue(representation.getFiles().isEmpty());
    }


    @Test
    public void shouldReturn404WhenDeletingNonExistingFile() {
        Response deleteFileResponse = fileWebTarget.request().delete();
        assertEquals("Unexpected status code", Response.Status.NOT_FOUND.getStatusCode(),
            deleteFileResponse.getStatus());
        ErrorInfo deleteErrorInfo = deleteFileResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());
    }


    @Test
    public void shouldUploadDataWithPut()
            throws Exception {
        // given particular (random in this case) content
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        // when content is added to record representation
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content),
                    MediaType.APPLICATION_OCTET_STREAM_TYPE);

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
            throws Exception {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        recordService.putContent(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file, new ByteArrayInputStream(
                content));

        // when this file is requested
        Response getFileResponse = fileWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getFileResponse.getStatus());

        // then concent should be equal to the previously put
        InputStream responseStream = getFileResponse.readEntity(InputStream.class);
        byte[] responseContent = ByteStreams.toByteArray(responseStream);
        assertArrayEquals("Read data is different from written", content, responseContent);
        assertEquals("File content tag mismatch", contentMd5, getFileResponse.getEntityTag().getValue());
    }
}
