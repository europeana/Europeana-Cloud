package eu.europeana.cloud.jobs;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 8/6/2018.
 */
public class VersionRemoverJobTest {

    private static final String CLOUD_ID = "CloudId";
    private static final String REPRESENTATION_NAME = "RepName";

    @Mock(name = "recordServiceClient")
    private RecordServiceClient recordServiceClient;
    private static final String VERSION = new com.eaio.uuid.UUID().toString();

    @InjectMocks
    private VersionRemoverJob versionRemoverJob = new VersionRemoverJob(recordServiceClient, new Representation(CLOUD_ID, REPRESENTATION_NAME, VERSION, null, null, null, null, null, true, null));

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }

    @Test
    public void shoudRemoveVersions() throws Exception {
        doNothing().when(recordServiceClient).deleteRepresentation(eq(CLOUD_ID), eq(REPRESENTATION_NAME), eq(VERSION));
        versionRemoverJob.call();
        verify(recordServiceClient, times(1)).deleteRepresentation(eq(CLOUD_ID), eq(REPRESENTATION_NAME), eq(VERSION));
    }

    @Test()
    public void shouldRetryThreeTimesBeforeException() throws Exception {
        doThrow(Exception.class).when(recordServiceClient).deleteRepresentation(eq(CLOUD_ID), eq(REPRESENTATION_NAME), eq(VERSION));
        try {
            versionRemoverJob.call();
            assertTrue(false);

        } catch (Exception e) {
            verify(recordServiceClient, times(3)).deleteRepresentation(eq(CLOUD_ID), eq(REPRESENTATION_NAME), eq(VERSION));
        }
    }


}