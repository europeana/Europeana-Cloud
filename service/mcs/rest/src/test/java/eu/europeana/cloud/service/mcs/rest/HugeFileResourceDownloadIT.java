package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.web.servlet.ResultActions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_RESOURCE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This tests checks if content is streamed (not put entirely into memory) when downloading file.
 */
@RunWith(CassandraTestRunner.class)
public class HugeFileResourceDownloadIT extends CassandraBasedAbstractResourceTest {

    private static RecordService recordService;

    private static final int HUGE_FILE_SIZE = 500_000_000;


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
        doAnswer(mockGetContent).when(recordService).getContent(anyString(), anyString(), anyString(), anyString(),
            anyLong(), anyLong(), any(OutputStream.class));
        Mockito.doReturn(file).when(recordService).getFile(globalId, schema, version, file.getFileName());

        // when we download mocked content of resource
        ResultActions response = mockMvc.perform(
                get(FILE_RESOURCE, globalId, schema, version, file.getFileName()))
                .andExpect(status().is2xxSuccessful());


        // then - we should be able to get full content and the content should have expected size
        int totalBytesInResponse = responseContentAsByteArray(response).length;
        assertEquals("Wrong size of read content", HUGE_FILE_SIZE, totalBytesInResponse);
    }


    private int getBytesCount(InputStream is)
            throws IOException {
        int totalBytes = 0;
        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = is.read(data, 0, data.length)) != -1) {
            totalBytes += nRead;
        }
        return totalBytes;
    }


    /**
     * Mock answer for
     * {@link ContentService#getContent(eu.europeana.cloud.common.model.Representation, eu.europeana.cloud.common.model.File, long, long, java.io.OutputStream)
     * getContent} method.
     */
    static class MockGetContentMethod implements Answer<Object> {

        final int totalBytes;


        public MockGetContentMethod(int totalBytes) {
            this.totalBytes = totalBytes;
        }


        @Override
        public Object answer(InvocationOnMock invocation)
                throws Throwable {
            Object[] args = invocation.getArguments();
            OutputStream os = (OutputStream) args[6];
            writeBytes(os);
            return null;
        }


        private void writeBytes(OutputStream os)
                throws IOException {
            for (int i = 0; i < totalBytes; i++) {
                os.write(1);
            }
        }
    }
}
