package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.junit.Assert.*;

/**
 * FileResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class FilesResourceTest extends JerseyTest {

    private RecordService recordService;

    private Representation rep;

    private File file;

    private WebTarget filesWebTarget;

    private UISClientHandler uisHandler;

    private static final byte[] XSLT_CONTENT = "<?xml version=\"1.0\"?><xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"></xsl:stylesheet>".getBytes();
    private static final byte[] XML_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?><sample></sample>".getBytes();
    private static final byte[] RDF_CONTENT = "<?xml version=\"1.0\"?><rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:si=\"https://www.w3schools.com/rdf/\"></rdf:RDF>".getBytes();

    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);

        uisHandler = applicationContext.getBean(UISClientHandler.class);
        Mockito.doReturn(new DataProvider()).when(uisHandler).getProvider(Mockito.anyString());
        Mockito.doReturn(true).when(uisHandler).existsCloudId(Mockito.anyString());
        DataProvider dp = new DataProvider();
        dp.setId("1");

        rep = recordService.createRepresentation("1", "1", "1");
        file = new File();
        file.setFileName("fileName");
        file.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

        Map<String, Object> allPathParams = ImmutableMap.<String, Object> of(ParamConstants.P_CLOUDID,
                rep.getCloudId(), ParamConstants.P_REPRESENTATIONNAME, rep.getRepresentationName(), ParamConstants.P_VER,
                rep.getVersion());
        filesWebTarget = target(FilesResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
    }


    @After
    public void cleanUp()
            throws Exception {
        try {
            recordService.deleteRepresentation(rep.getCloudId(), rep.getRepresentationName());
        }catch (Exception e){
            // do nothing it's cleaning step
        }
    }


    @Override
    public Application configure() {
        return null; //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }


    @Test
    public void shouldUploadDataWithPostWithoutFileName()
            throws Exception {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        // when content is added to record representation
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content),
                        APPLICATION_OCTET_STREAM_TYPE);

        Response postFileResponse = filesWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), postFileResponse.getStatus());
        assertEquals("File content tag mismatch", contentMd5, postFileResponse.getEntityTag().getValue
                ());

        // then data should be in record service
        rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        assertEquals(1, rep.getFiles().size());

        File insertedFile = rep.getFiles().get(0);
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
                insertedFile.getFileName(), contentBos);
        assertEquals("MD5 file mismatch", contentMd5, insertedFile.getMd5());
        assertEquals(content.length, insertedFile.getContentLength());
        assertArrayEquals(content, contentBos.toByteArray());
    }


    @Test
    public void shouldUploadDataWithPostWithFileName()
            throws Exception {
        // given particular (random in this case) content in service
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        // when content is added to record representation
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content),
                        APPLICATION_OCTET_STREAM_TYPE).field(ParamConstants.F_FILE_NAME, file.getFileName());

        Response postFileResponse = filesWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), postFileResponse.getStatus());
        assertEquals("File content tag mismatch", contentMd5, postFileResponse.getEntityTag().getValue());

        // then data should be in record service
        rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        assertEquals(1, rep.getFiles().size());

        File insertedFile = rep.getFiles().get(0);
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
                insertedFile.getFileName(), contentBos);
        assertEquals("FileName mismatch", file.getFileName(), insertedFile.getFileName());
        assertEquals("MD5 file mismatch", contentMd5, insertedFile.getMd5());
        assertEquals(content.length, insertedFile.getContentLength());
        assertArrayEquals(content, contentBos.toByteArray());
    }

    @Test
    public void shouldBeReturn409WhenFileAlreadyExist()
            throws Exception {
        // given particular (random in this case) content in service
        byte[] content = { 1, 2, 3, 4 };
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        recordService.putContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file,
                new ByteArrayInputStream(content));

        byte[] modifiedContent = { 5, 6, 7 };
        ThreadLocalRandom.current().nextBytes(modifiedContent);
        String modifiedContentMd5 = Hashing.md5().hashBytes(content).toString();
        // when content is added to record representation

        ByteArrayInputStream modifiedInputStream = new ByteArrayInputStream(modifiedContent);
        MediaType detect = getMediaType(modifiedInputStream);
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_FILE_MIME, detect.toString())
                .field(ParamConstants.F_FILE_DATA, modifiedInputStream,
                        detect).field(ParamConstants.F_FILE_NAME, file.getFileName());

        Response postFileResponse = filesWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code" + postFileResponse.readEntity(String.class), Response.Status
                        .CONFLICT
                        .getStatusCode(),
                postFileResponse.getStatus());

        // then data should be in record service
        rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        assertEquals(1, rep.getFiles().size());

        File insertedFile = rep.getFiles().get(0);
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
                insertedFile.getFileName(), contentBos);
        assertNotSame("MD5 file mismatch", modifiedContentMd5, insertedFile.getMd5());
        assertNotSame(modifiedContent.length, insertedFile.getContentLength());
    }

    private MediaType getMediaType(ByteArrayInputStream modifiedInputStream) throws IOException {
        return MediaType.valueOf(new AutoDetectParser().getDetector().detect(modifiedInputStream, new
                Metadata()).toString());
    }

    @Test
    public void shouldUploadXMLFileWithApplicationXMLMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XML_CONTENT,"application/xml");
    }

    @Test
    public void shouldUploadXMLFileWithTextXMLMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XML_CONTENT,"text/xml");
    }

    @Test
    public void shouldUploadXMLFileWithTextPlainMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XML_CONTENT,"text/plain");
    }

    @Test
    public void shouldUploadRdfFileWithTextXmlMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(RDF_CONTENT,"text/xml");
    }

    @Test
    public void shouldUploadRdfFileWithTextPlainMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(RDF_CONTENT,"text/plain");
    }

    @Test
    public void shouldUploadRdfFileWithApplicationXmlMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(RDF_CONTENT,"application/xml");
    }

    @Test
    public void shouldUploadXsltFileWithTextPlainMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XSLT_CONTENT,"text/plain");
    }

    @Test
    public void shouldUploadXsltFileWithTextXmlMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XSLT_CONTENT,"text/xml");
    }

    @Test
    public void shouldUploadXsltFileWithApplicationXmlMimeType()
            throws Exception {
        uploadFileWithGivenMimeType(XSLT_CONTENT,"application/xml");
    }

    private void uploadFileWithGivenMimeType(byte[] fileContent, String mimeType) throws RepresentationNotExistsException, FileNotExistsException {
        String contentMd5 = Hashing.md5().hashBytes(fileContent).toString();

        // when content is added to record representation
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mimeType)
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(fileContent),
                        APPLICATION_OCTET_STREAM_TYPE);

        Response postFileResponse = filesWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unexpected status code", Response.Status.CREATED.getStatusCode(), postFileResponse.getStatus());
        assertEquals("File content tag mismatch", contentMd5, postFileResponse.getEntityTag().getValue
                ());

        // then data should be in record service
        rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        assertEquals(1, rep.getFiles().size());

        File insertedFile = rep.getFiles().get(0);
        ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
        recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
                insertedFile.getFileName(), contentBos);
        assertEquals("MD5 file mismatch", contentMd5, insertedFile.getMd5());
        assertEquals(fileContent.length, insertedFile.getContentLength());
        assertArrayEquals(fileContent, contentBos.toByteArray());
    }
}
