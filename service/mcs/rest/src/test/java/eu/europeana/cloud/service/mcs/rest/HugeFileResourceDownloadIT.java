package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;

/**
 * This tests checks if content is streamed (not put entirely into memory) when downloading file.
 */
@RunWith(CassandraTestRunner.class)
public class HugeFileResourceDownloadIT extends JerseyTest {

    private static RecordService recordService;

    private static final int HUGE_FILE_SIZE = 1 << 30;


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
        return null; //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
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
        WebTarget webTarget = target(FileResource.class.getAnnotation(Path.class).value()) //
                .resolveTemplates(ImmutableMap.<String, Object> of( //
                    ParamConstants.P_CLOUDID, globalId, //
                    ParamConstants.P_REPRESENTATIONNAME, schema, //
                    ParamConstants.P_VER, version, //
                    ParamConstants.P_FILENAME, file.getFileName()));

        Response response = webTarget.request().get();
        assertEquals("Unsuccessful request", Response.Status.Family.SUCCESSFUL, response.getStatusInfo().getFamily());

        // then - we should be able to get full content and the content should have expected size
        InputStream responseStream = response.readEntity(InputStream.class);
        int totalBytesInResponse = getBytesCount(responseStream);
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
