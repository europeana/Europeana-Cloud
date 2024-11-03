package eu.europeana.cloud.service.mcs.controller;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestRunner;
import eu.europeana.cloud.test.S3TestHelper;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static jakarta.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.junit.Assert.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FileResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class FileResourceTest extends CassandraBasedAbstractResourceTest {

  private RecordService recordService;

  private Representation rep;

  private File file;

  private String fileWebTarget;

  private UISClientHandler uisHandler;

  private DataSetService dataSetService;

  private DataSetPermissionsVerifier dataSetPermissionsVerifier;

  @BeforeClass
  public static void setUp(){
    S3TestHelper.startS3MockServer();
  }
  @Before
  public void mockUp() throws Exception {
    recordService = applicationContext.getBean(RecordService.class);
    uisHandler = applicationContext.getBean(UISClientHandler.class);
    dataSetService = applicationContext.getBean(DataSetService.class);
    dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);
    DataProvider dataProvider = new DataProvider();
    dataProvider.setId("1");
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider("1");
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToAddRevisionTo(Mockito.any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDeleteFileFor(Mockito.any());

    dataSetService.createDataSet("1", "s", "desc");
    rep = recordService.createRepresentation("1", "1", "1", "s");
    file = new File();
    file.setFileName("fileName");
    file.setMimeType(APPLICATION_OCTET_STREAM_TYPE.toString());

    fileWebTarget =
        "/records/" + rep.getCloudId() + "/representations/" + rep.getRepresentationName() + "/versions/" + rep.getVersion()
            + "/files/" + file.getFileName();
  }

  @After
  public void cleanUp() throws Exception {
    recordService.deleteRepresentation(rep.getCloudId(),
        rep.getRepresentationName());
    S3TestHelper.cleanUpBetweenTests();
  }

  @AfterClass
  public static void cleanUpAfterTests() {
    S3TestHelper.stopS3MockServer();
  }

  @Test
  public void shouldReturnContentWithinRangeOffset() throws Exception {
    // given particular content in service
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when part of file is requested (skip first byte)
    ResultActions response = mockMvc.perform(
        get(fileWebTarget)
            .header("Range", "bytes=1-")).andExpect(status().isPartialContent());

    response.andReturn().getAsyncResult();

    // then retrieved content should consist of second and third byte of
    // inserted byte array
    byte[] responseContent = responseContentAsByteArray(response);

    byte[] expectedResponseContent = copyOfRange(content, 1,
        content.length - 1);
    assertArrayEquals("Read data is different from requested range",
        expectedResponseContent, responseContent);
  }

  int[][] parameters = new int[][]{{1, 2}, {0, 0}, {0, 1}, {3, 3},
      {0, 3}, {3, 4}};

  /**
   * Yeah, this test really sucks. It should use Parameterized test (as it was implemented earlier - see the anotaions down below
   * commented out). But it is impossible to use 2 runners in one test.
   *
   * @throws Exception
   */
  @Test
  public void shouldReturnContentWithinRangeForParameters() throws Exception {
    for (int[] elem : parameters) {
      shouldReturnContentWithinRange(elem[0], elem[1]);
    }
  }

  // @Test
  // @Parameters({ "1,2", "0,0", "0,1", "3,3", "0,3", "3,4" })
  public void shouldReturnContentWithinRange(Integer rangeStart,
      Integer rangeEnd) throws Exception {
    // given particular content in service
    byte[] content = {1, 2, 3, 4, 5};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when part of file is requested (2 bytes with 1 byte offset)
    ResultActions response = mockMvc.perform(get(fileWebTarget)
                                        .header("Range", String.format("bytes=%d-%d", rangeStart, rangeEnd)))
                                    .andExpect(status().isPartialContent());

    response.andReturn().getAsyncResult();

    // then retrieved content should consist of second and third byte of
    // inserted byte array

    byte[] responseContent = responseContentAsByteArray(response);
    byte[] expectedResponseContent = copyOfRange(content, rangeStart,
        rangeEnd);
    assertArrayEquals("Read data is different from requested range",
        expectedResponseContent, responseContent);
  }

  /**
   * Copy the specified range of array to a new array. This method works similar to {@link Arrays#copyOfRange(byte[], int, int)},
   * but final index is inclusive.
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
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when unsatisfiable content range is requested
    // then should response that requested range is not satisfiable
    ResultActions result = mockMvc.perform(get(fileWebTarget)
                                      .header("Range", "bytes=4-5"))
                                  .andDo(print());
    result
        .andExpect(status().isRequestedRangeNotSatisfiable());
  }

  @Test
  public void shouldReturnErrorWhenRequestedRangeNotValid() throws Exception {
    // given particular content in service
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when part of file is requested (2 bytes with 1 byte offset)
    // then should response that request is wrongly formatted
    mockMvc.perform(get(fileWebTarget)
               .header("Range", "bytes=-2"))
           .andExpect(status().isRequestedRangeNotSatisfiable());
  }

  @Test
  @Ignore(value = "TODO: implement")
  public void shouldReturnErrorOnHashMismatch() {
  }

  @Test
  public void shouldOverrideFileOnRepeatedPut() throws Exception {
    // given particular content in service
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when you override it with another content
    byte[] contentModified = {5, 6, 7};
    String contentModifiedMd5 = Hashing.md5().hashBytes(contentModified)
                                       .toString();

    mockMvc.perform(putFile(fileWebTarget, file.getMimeType(), contentModified))
           .andExpect(status().isNoContent());

    // then the content in service should be also modivied
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String retrievedFileMd5 = recordService.getContent(rep.getCloudId(),
        rep.getRepresentationName(), rep.getVersion(),
        file.getFileName(), baos);
    assertArrayEquals("Read data is different from written",
        contentModified, baos.toByteArray());
    assertEquals("MD5 checksum is different than written",
        contentModifiedMd5, retrievedFileMd5);
  }

  @Test
  public void shouldDeleteFile() throws Exception {
    // given particular (random in this case) content in service
    byte[] content = new byte[1000];
    ThreadLocalRandom.current().nextBytes(content);
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    mockMvc.perform(delete(fileWebTarget)).andExpect(status().isNoContent());

    Representation representation = recordService
        .getRepresentation(rep.getCloudId(),
            rep.getRepresentationName(), rep.getVersion());
    assertTrue(representation.getFiles().isEmpty());
  }

  @Test
  public void shouldReturn404WhenDeletingNonExistingFile() throws Exception {
    ResultActions response = mockMvc.perform(delete(fileWebTarget)).andExpect(status().isNotFound());

    ErrorInfo deleteErrorInfo = responseContentAsErrorInfo(response);
    assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(),
        deleteErrorInfo.getErrorCode());
  }

  @Test
  public void shouldReturn404WhenDeletingNonExistingFileWithExtensionAcceptJson() throws Exception {
    ResultActions response = mockMvc.perform(delete(fileWebTarget + ".txt").accept(MediaType.APPLICATION_JSON))
                                    .andExpect(status().isNotFound());

    ErrorInfo deleteErrorInfo = responseContentAsErrorInfo(response, MediaType.APPLICATION_JSON);
    assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());
  }

  @Test
  public void shouldReturn404WhenDeletingNonExistingFileWithExtensionAcceptXml() throws Exception {
    ResultActions response = mockMvc.perform(delete(fileWebTarget + ".txt").accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().isNotFound());

    ErrorInfo deleteErrorInfo = responseContentAsErrorInfo(response, MediaType.APPLICATION_XML);
    assertEquals(McsErrorCode.FILE_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());
  }

  @Test
  public void shouldReturn404WhenUpdatingNotExistingFile() throws Exception {
    // given particular (random in this case) content
    byte[] content = new byte[1000];
    ThreadLocalRandom.current().nextBytes(content);

    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(Mockito.any());

    // when content is added to record representation
    mockMvc.perform(putFile(fileWebTarget, file.getMimeType(), content))
           .andExpect(status().isMethodNotAllowed());
  }

  @Test
  public void shouldRetrieveContent() throws Exception {
    // given particular (random in this case) content in service
    byte[] content = new byte[1000];
    ThreadLocalRandom.current().nextBytes(content);
    String contentMd5 = Hashing.md5().hashBytes(content).toString();
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when this file is requested
    ResultActions response = mockMvc.perform(get(fileWebTarget))
                                    .andExpect(status().isOk())
                                    .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

    response.andReturn().getAsyncResult();

    // then concent should be equal to the previously put
    byte[] responseContent = responseContentAsByteArray(response);
    assertArrayEquals("Read data is different from written", content, responseContent);
  }

  @Test
  public void shouldReturnCorrectHeaderForHeadRequest() throws Exception {
    byte[] content = {1, 2, 3, 4};
    recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
        rep.getVersion(), file, new ByteArrayInputStream(content));

    // when part of file is requested (skip first byte)
    ResultActions getFileResponse = mockMvc.perform(head(fileWebTarget)).andExpect(status().isOk());
    String locationHeader = getFileResponse.andReturn().getResponse().getHeader(HttpHeaders.LOCATION);
    String fileUri = getFileResponse.andReturn().getRequest().getRequestURI();
    assertEquals(locationHeader, fileUri);
  }
}
