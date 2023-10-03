package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.CLIENT_FILE_RESOURCE;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsByteArray;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.ResultActions;

/**
 * This tests checks if content is streamed (not put entirely into memory) when downloading file.
 */
public class HugeFileResourceDownloadIT extends AbstractResourceTest {

  private static RecordService recordService;

  private static final int HUGE_FILE_SIZE = 200_000_000;

  @Before
  public void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
  }

  @After
  public void cleanUp() {
    reset(recordService);
  }

  @Test
  public void shouldHandleHugeFile()
      throws Exception {
    // given representation with file in service
    String globalId = "globalId", schema = "schema", version = "v1";
    MockGetContentMethod mockGetContent = new MockGetContentMethod(HUGE_FILE_SIZE);
    File file = new File("fileName", "text/plain", "md5", new Date().toString(), HUGE_FILE_SIZE, null);

    // mock answers:
    doReturn(mockGetContent).when(recordService).getContent(anyString(), anyString(), anyString(), anyString(),
        anyLong(), anyLong());
    Mockito.doReturn(file).when(recordService).getFile(globalId, schema, version, file.getFileName());

    // when we download mocked content of resource
    ResultActions response = mockMvc.perform(
                                        get(CLIENT_FILE_RESOURCE, globalId, schema, version, file.getFileName()))
                                    .andExpect(status().is2xxSuccessful());

    response.andReturn().getAsyncResult();

    // then - we should be able to get full content and the content should have expected size
    int totalBytesInResponse = responseContentAsByteArray(response).length;
    assertEquals("Wrong size of read content", HUGE_FILE_SIZE, totalBytesInResponse);
  }

  /**
   * Mock answer for {@link RecordService#getContent(String, String, String, String, long, long) getContent} method.
   */
  static class MockGetContentMethod implements Consumer<OutputStream> {

    final int totalBytes;

    public MockGetContentMethod(int totalBytes) {
      this.totalBytes = totalBytes;
    }

    public void accept(OutputStream os) {
      try {
        for (int i = 0; i < totalBytes; i++) {
          os.write(1);
        }
      } catch (IOException e) {
        throw new SystemException(e);
      }
    }
  }
}
