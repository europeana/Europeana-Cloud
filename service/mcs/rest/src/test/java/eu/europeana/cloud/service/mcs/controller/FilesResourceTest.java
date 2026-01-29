package eu.europeana.cloud.service.mcs.controller;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestExtension;
import eu.europeana.cloud.test.S3TestHelper;
import jakarta.ws.rs.core.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.common.web.ParamConstants.PROVIDER_ID;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.isEtag;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.postFile;
import static jakarta.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FileResourceTest
 */
@ExtendWith(CassandraTestExtension.class)
public class FilesResourceTest extends CassandraBasedAbstractResourceTest {

  private RecordService recordService;
  private DataSetService dataSetService;

  private Representation rep;

  private File file;

  private String filesWebTarget;

  private UISClientHandler uisHandler;

  private DataSetPermissionsVerifier dataSetPermissionsVerifier;

  private static final byte[] XSLT_CONTENT = "<?xml version=\"1.0\"?><xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"></xsl:stylesheet>".getBytes();
  private static final byte[] XML_CONTENT = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?><sample></sample>".getBytes();
  private static final byte[] RDF_CONTENT = "<?xml version=\"1.0\"?><rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:si=\"https://www.w3schools.com/rdf/\"></rdf:RDF>".getBytes();


    @BeforeAll
    static void setUp() {
      S3TestHelper.startS3MockServer();
    }

  @BeforeEach
  void mockUp()
          throws Exception {
    recordService = applicationContext.getBean(RecordService.class);
    dataSetService = applicationContext.getBean(DataSetService.class);

    uisHandler = applicationContext.getBean(UISClientHandler.class);
    dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);
    Mockito.doReturn(new DataProvider()).when(uisHandler).getProvider(Mockito.anyString());
    Mockito.doReturn(true).when(uisHandler).existsCloudId(Mockito.anyString());

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());

    DataProvider dp = new DataProvider();
    dp.setId("1");

    dataSetService.createDataSet(PROVIDER_ID, DATA_SET_ID, "");
    rep = recordService.createRepresentation("1", "1", PROVIDER_ID, DATA_SET_ID);

    file = new File();
    file.setFileName("fileName");
    file.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

    filesWebTarget =
        "/records/" + rep.getCloudId() + "/representations/" + rep.getRepresentationName() + "/versions/" + rep.getVersion()
            + "/files";

    Mockito.doReturn(true).when(permissionEvaluator)
           .hasPermission(any(), any(), any(), any());
  }


  @AfterEach
  void cleanUp() {
    try {
      recordService.deleteRepresentation(rep.getCloudId(), rep.getRepresentationName());
      S3TestHelper.cleanUpBetweenTests();
    } catch (Exception e) {
      // do nothing it's cleaning step
    }
  }

  @AfterAll
  static void cleanUpAfterTests() {
    S3TestHelper.stopS3MockServer();
  }

  @Test
  void shouldUploadDataWithPostWithoutFileName()
          throws Exception {
    // given particular (random in this case) content in service
    byte[] content = new byte[1000];
    ThreadLocalRandom.current().nextBytes(content);
    String contentMd5 = Hashing.md5().hashBytes(content).toString();

    // when content is added to record representation

    mockMvc.perform(postFile(filesWebTarget, file.getMimeType(), content))
            .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

    // then data should be in record service
    rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
    assertEquals(1, rep.getFiles().size());

    File insertedFile = rep.getFiles().get(0);
    ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
      recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
              insertedFile.getFileName(), contentBos);
      assertEquals(contentMd5, insertedFile.getMd5());
      assertEquals(content.length, insertedFile.getContentLength());
    assertArrayEquals(content, contentBos.toByteArray());
  }


  @Test
  void shouldUploadDataWithPostWithFileName()
          throws Exception {
    // given particular (random in this case) content in service
    byte[] content = new byte[1000];
    ThreadLocalRandom.current().nextBytes(content);
    String contentMd5 = Hashing.md5().hashBytes(content).toString();

    // when content is added to record representation
    mockMvc.perform(postFile(filesWebTarget, file.getMimeType(), content)
                    .param(ParamConstants.F_FILE_NAME, file.getFileName()))
            .andExpect(status().isCreated())
            .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

      // then data should be in record service
      rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
      assertEquals(1, rep.getFiles().size());

      File insertedFile = rep.getFiles().get(0);
      ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
      recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
              insertedFile.getFileName(), contentBos);
      assertEquals(file.getFileName(), insertedFile.getFileName());
      assertEquals(contentMd5, insertedFile.getMd5());
      assertEquals(content.length, insertedFile.getContentLength());
      assertArrayEquals(content, contentBos.toByteArray());
  }

  @Test
  void shouldBeReturn409WhenFileAlreadyExist()
          throws Exception {
    // given particular (random in this case) content in service
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file,
            new ByteArrayInputStream(content));

    byte[] modifiedContent = {5, 6, 7};
    ThreadLocalRandom.current().nextBytes(modifiedContent);
    String modifiedContentMd5 = Hashing.md5().hashBytes(content).toString();

    // when content is added to record representation
    ByteArrayInputStream modifiedInputStream = new ByteArrayInputStream(modifiedContent);
    MediaType detect = getMediaType(modifiedInputStream);

    mockMvc.perform(postFile(filesWebTarget, detect.toString(), modifiedContent).
               param(ParamConstants.F_FILE_NAME, file.getFileName()))
           .andExpect(status().isConflict());

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
  void shouldUploadXMLFileWithApplicationXMLMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XML_CONTENT, "application/xml");
  }

  @Test
  void shouldUploadXMLFileWithTextXMLMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XML_CONTENT, "text/xml");
  }

  @Test
  void shouldUploadXMLFileWithTextPlainMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XML_CONTENT, "text/plain");
  }

  @Test
  void shouldUploadRdfFileWithTextXmlMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(RDF_CONTENT, "text/xml");
  }

  @Test
  void shouldUploadRdfFileWithTextPlainMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(RDF_CONTENT, "text/plain");
  }

  @Test
  void shouldUploadRdfFileWithApplicationXmlMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(RDF_CONTENT, "application/xml");
  }

  @Test
  void shouldUploadXsltFileWithTextPlainMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XSLT_CONTENT, "text/plain");
  }

  @Test
  void shouldUploadXsltFileWithTextXmlMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XSLT_CONTENT, "text/xml");
  }

  @Test
  void shouldUploadXsltFileWithApplicationXmlMimeType()
          throws Exception {
    uploadFileWithGivenMimeType(XSLT_CONTENT, "application/xml");
  }

  private void uploadFileWithGivenMimeType(byte[] fileContent, String mimeType) throws Exception {
    String contentMd5 = Hashing.md5().hashBytes(fileContent).toString();

    // when content is added to record representation
    mockMvc.perform(postFile(filesWebTarget, mimeType, fileContent))
           .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

    // then data should be in record service
    rep = recordService.getRepresentation(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
    assertEquals(1, rep.getFiles().size());

    File insertedFile = rep.getFiles().get(0);
    ByteArrayOutputStream contentBos = new ByteArrayOutputStream();
      recordService.getContent(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
              insertedFile.getFileName(), contentBos);
      assertEquals(contentMd5, insertedFile.getMd5());
      assertEquals(fileContent.length, insertedFile.getContentLength());
    assertArrayEquals(fileContent, contentBos.toByteArray());
  }
}
