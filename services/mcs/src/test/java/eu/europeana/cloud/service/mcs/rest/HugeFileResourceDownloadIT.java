package eu.europeana.cloud.service.mcs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.ContentService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.test.ChunkedHttpUrlConnector;

/**
 * FileResourceTest
 */
public class HugeFileResourceDownloadIT extends JerseyTest {

    private static RecordService recordService;

    private static ContentService contentService;

    private Representation recordRepresentation;

    private static final int HUGE_FILE_SIZE = 1 << 30;


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        contentService = applicationContext.getBean(ContentService.class);

        recordRepresentation = recordService.createRepresentation("1", "1", "1");
    }


    @After
    public void cleanUp() {
        reset(recordService);
        recordService.deleteRepresentation(recordRepresentation.getRecordId(), recordRepresentation.getSchema());
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }


    @Test
    public void testDownloadHugeFile()
            throws FileNotExistsException, IOException {
        File f = new File();
        f.setFileName("terefere");
        recordRepresentation = recordService.addFileToRepresentation(recordRepresentation.getRecordId(), recordRepresentation.getSchema(), recordRepresentation.getVersion(), f);
        MockGetContentMethod mockGetContent = new MockGetContentMethod(HUGE_FILE_SIZE);
        doAnswer(mockGetContent).when(contentService).getContent(any(Representation.class), any(File.class), anyLong(),
                anyLong(), any(OutputStream.class));

        WebTarget webTarget = target(FileResource.class.getAnnotation(Path.class).value())
                .resolveTemplates(ImmutableMap.<String, Object>of(
                ParamConstants.P_GID, recordRepresentation.getRecordId(),
                ParamConstants.P_REP, recordRepresentation.getSchema(),
                ParamConstants.P_VER, recordRepresentation.getVersion(),
                ParamConstants.P_FILE, f.getFileName()));

        Response response = webTarget.request().get();
        assertEquals("Unsuccessful request", Response.Status.Family.SUCCESSFUL, response.getStatusInfo().getFamily());

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
     * Mock answer for {@link ContentService#getContent(eu.europeana.cloud.common.model.Representation, 
     * eu.europeana.cloud.common.model.File, long, long, java.io.OutputStream)  getContent} method.     
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
            OutputStream os = (OutputStream) args[4];
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
