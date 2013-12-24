package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.test.ChunkedHttpUrlConnector;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationContext;

/**
 * This tests checks if content is streamed (not put entirely into memory) when uploading file.
 */
public class HugeFileResourceUploadIT extends JerseyTest {

    private static RecordService recordService;

    private static final int HUGE_FILE_SIZE = 1 << 30;


    @BeforeClass
    public static void cleanUpAfterPreviousTest()
            throws InterruptedException {
        // not sure why it's needed - but without it, this test fails (something isn't cleaned after previous test)
        TimeUnit.SECONDS.sleep(5);
    }


    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
    }


    @After
    public void cleanUp()
            throws Exception {
        reset(recordService);
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
        config.property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024);
        config.connector(new ChunkedHttpUrlConnector(config));
    }


    @Test
    public void testUploadingHugeFile()
            throws Exception {
        String globalId = "globalId", schema = "schema", version = "v1";

        // mock answers
        MockPutContentMethod mockPutContent = new MockPutContentMethod();
        doAnswer(mockPutContent).when(recordService).putContent(anyString(), anyString(), anyString(), any(File.class),
            any(InputStream.class));
        WebTarget webTarget = target(FileResource.class.getAnnotation(Path.class).value()).resolveTemplates(
            ImmutableMap.<String, Object> of( //
                ParamConstants.P_GID, globalId, //
                ParamConstants.P_SCHEMA, schema, //
                ParamConstants.P_VER, version, //
                ParamConstants.P_FILE, "terefere"));

        MessageDigest md = MessageDigest.getInstance("MD5");
        DigestInputStream inputStream = new DigestInputStream(new DummyStream(HUGE_FILE_SIZE), md);

        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME,
            MediaType.APPLICATION_OCTET_STREAM).field(ParamConstants.F_FILE_DATA, inputStream,
            MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response response = webTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unsuccessful request", Response.Status.Family.SUCCESSFUL, response.getStatusInfo().getFamily());
        assertEquals("Wrong size of read content", HUGE_FILE_SIZE, mockPutContent.totalBytes);

        String contentMd5Hex = BaseEncoding.base16().lowerCase().encode(md.digest());
        assertEquals("Content hash mismatch", contentMd5Hex, response.getEntityTag().getValue());
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
