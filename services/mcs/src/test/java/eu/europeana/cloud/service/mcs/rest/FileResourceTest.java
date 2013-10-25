package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;

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
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.service.ContentService;
import eu.europeana.cloud.service.mcs.service.RecordService;
import eu.europeana.cloud.test.ChunkedHttpUrlConnector;

/**
 * FileResourceTest
 */
public class FileResourceTest extends JerseyTest {

    private RecordService recordService;

    private ContentService contentService;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        contentService = applicationContext.getBean(ContentService.class);
        reset(recordService);
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:fileResourceTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class)
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024);
        config.connector(new ChunkedHttpUrlConnector(config));
    }


    @Test
    public void testUploadingHugeFile()
            throws IOException {
        MockWriteInputStreamMethod mockMethod = new MockWriteInputStreamMethod();
        doAnswer(mockMethod).when(contentService).insertContent(any(Representation.class), any(File.class), any(InputStream.class));

        Representation rep = recordService.createRepresentation("1", "1", "1");

        WebTarget webTarget = target("/records/{ID}/representations/{REPRESENTATION}/versions/{VERSION}/files/{FILE}")
                .resolveTemplates(ImmutableMap.<String, Object>of(
                "ID", rep.getRecordId(),
                "REPRESENTATION", rep.getSchema(),
                "VERSION", rep.getVersion(),
                "FILE", "terefere"));

        final int dummyStreamSize = 1024 * 1024 * 1024;
        InputStream inputStream = new DummyStream(dummyStreamSize);

        FormDataMultiPart multipart = new FormDataMultiPart()
                .field("mimeType", MediaType.APPLICATION_OCTET_STREAM)
                .field("data", inputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE);

        Response resp = webTarget.request().put(Entity.entity(multipart, multipart.getMediaType()));
        assertEquals("Unsuccessful request", Response.Status.Family.SUCCESSFUL, resp.getStatusInfo().getFamily());
        assertEquals("Wrong size of read content", dummyStreamSize, mockMethod.totalBytes);
    }

    static class MockWriteInputStreamMethod implements Answer<Object> {

        int totalBytes;


        @Override
        public Object answer(InvocationOnMock invocation)
                throws Throwable {
            Object[] args = invocation.getArguments();
            InputStream inputStream = (InputStream) args[2];
            consume(inputStream);
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

    static class DummyStream extends InputStream {

        private final int totalLength;

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
