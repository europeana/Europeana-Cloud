package eu.europeana.cloud.service.mcs.rest;

import com.google.common.io.BaseEncoding;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This tests checks if content is streamed (not put entirely into memory) when uploading file.
 */
@RunWith(CassandraTestRunner.class)
public class HugeFileResourceUploadIT extends CassandraBasedAbstractResourceTest {

    private static RecordService recordService;

    private static final int HUGE_FILE_SIZE = 500_000_000;
    private static final String FILES_RESOURCE_PATH = FilesResource.class.getAnnotation(RequestMapping.class).value()[0];


    @Before
    public void mockUp() {
        recordService = applicationContext.getBean(RecordService.class);
    }


    @After
    public void cleanUp() {
        reset(recordService);
    }

    //new JerseyConfig().property("contextConfigLocation", "classpath:hugeFileResourceTestContext.xml");


//    @Override
//    protected void configureClient(ClientConfig config) {
//        config.register(MultiPartFeature.class);
//        config.property(ClientProperties.REQUEST_ENTITY_PROCESSING,
//                RequestEntityProcessing.CHUNKED);
//    }


    @Test
    public void testUploadingHugeFile()
            throws Exception {
        String globalId = "globalId", schema = "schema", version = "v1";

        // mock answers
        MockPutContentMethod mockPutContent = new MockPutContentMethod();
        doAnswer(mockPutContent).when(recordService).putContent(anyString(), anyString(), anyString(), any(File.class),
                any(InputStream.class));


        MessageDigest md = MessageDigest.getInstance("MD5");
        DigestInputStream inputStream = new DigestInputStream(new DummyStream(HUGE_FILE_SIZE), md);



        MockMultipartFile multipart = new MockMultipartFile("x", null, MediaType.APPLICATION_OCTET_STREAM_VALUE, inputStream);

        ResultActions response = mockMvc.perform(multipart(FILES_RESOURCE_PATH, globalId,schema, version).file(multipart).contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM))
                .andExpect(status().is2xxSuccessful());

        assertEquals("Wrong size of read content", HUGE_FILE_SIZE, mockPutContent.totalBytes);
        String contentMd5Hex = BaseEncoding.base16().lowerCase().encode(md.digest());
        response.andExpect(header().string(HttpHeaders.ETAG,contentMd5Hex));
    }


    /**
     * Mock answer for
     * {@link ContentService#putContent(eu.europeana.cloud.common.model.Representation, eu.europeana.cloud.common.model.File, java.io.InputStream)
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
        public int read()
                throws IOException {
            if (readLength >= totalLength) {
                return -1;
            } else {
                readLength++;
                return 1;
            }
        }
    }
}
