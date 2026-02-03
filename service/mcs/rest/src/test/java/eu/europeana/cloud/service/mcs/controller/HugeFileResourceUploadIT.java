package eu.europeana.cloud.service.mcs.controller;

import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILES_RESOURCE;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.isEtag;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.postFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This tests checks if content is streamed (not put entirely into memory) when uploading file.
 */
@ExtendWith(CassandraTestExtension.class)
class HugeFileResourceUploadIT extends CassandraBasedAbstractResourceTest {

  private static RecordService recordService;
  private static DataSetPermissionsVerifier dataSetPermissionsVerifier;

  private static final int HUGE_FILE_SIZE = 200_000_000;

  @BeforeEach
  void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
    dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);

    }

  @AfterEach
  void cleanUp() {
    reset(recordService);
  }

  @Test
  void testUploadingHugeFile()
          throws Exception {
    String globalId = "globalId", schema = "schema", version = "v1";

    // mock answers
    MockPutContentMethod mockPutContent = new MockPutContentMethod();
    doAnswer(mockPutContent).when(recordService).putContent(anyString(), anyString(), anyString(), any(File.class),
            any(InputStream.class));
    doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToUploadFileFor(any(Representation.class));
    MessageDigest md = MessageDigest.getInstance("MD5");
    DigestInputStream inputStream = new DigestInputStream(new DummyStream(HUGE_FILE_SIZE), md);

    String target = UriComponentsBuilder.fromUriString(FILES_RESOURCE).build(globalId, schema, version).toString();
    byte[] content = FileCopyUtils.copyToByteArray(inputStream);
    ResultActions response = mockMvc.perform(postFile(target, MediaType.APPLICATION_OCTET_STREAM_VALUE, content))
                                    .andExpect(status().is2xxSuccessful());

      assertEquals(HUGE_FILE_SIZE, mockPutContent.totalBytes);
    String contentMd5Hex = BaseEncoding.base16().lowerCase().encode(md.digest());
    response.andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5Hex)));
  }

  /**
   * Mock answer for
   * {@link RecordService#putContent(String, String, String, eu.europeana.cloud.common.model.File, java.io.InputStream)
   * putContent} method. Only counts bytes in input stream.
   */
  static class MockPutContentMethod implements Answer<Object> {

    int totalBytes;

    @Override
    public Object answer(InvocationOnMock invocation)
        throws Throwable {
      Object[] args = invocation.getArguments();
      File file = (File) args[3];
      MessageDigest md = MessageDigest.getInstance("MD5");
      DigestInputStream inputStream = new DigestInputStream((InputStream) args[4], md);
      consume(inputStream);
      file.setFileName("terefere");
      file.setMd5(BaseEncoding.base16().lowerCase().encode(md.digest()));
      return null;
    }


    private void consume(InputStream is)
        throws IOException {

      int nRead;
      byte[] data = new byte[16384];

      while ((nRead = is.read(data, 0, data.length)) != -1) {
        totalBytes += nRead;
      }
    }
  }


  /**
   * Input stream that generates unspecified bytes. The total length of stream is specified in constuctor.
   */
  static class DummyStream extends InputStream {

    /**
     * Total number of bytes that will be produced in this stream.
     */
    private final int totalLength;

    /**
     * Number of bytes that have already been read from this stream.
     */
    private int readLength = 0;


    public DummyStream(int totalLength) {
      this.totalLength = totalLength;
    }


    @Override
    public int read() {
      if (readLength >= totalLength) {
        return -1;
      } else {
        readLength++;
        return 1;
      }
    }
  }
}
