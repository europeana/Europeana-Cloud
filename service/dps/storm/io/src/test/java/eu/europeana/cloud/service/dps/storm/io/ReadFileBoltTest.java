package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.mockito.Matchers.*;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/9/2018.
 */
@RunWith(MockitoJUnitRunner.class)
public class ReadFileBoltTest {

    private static final String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";
    private static final String AUTHORIZATION = "Authorization";
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "fileClient")
    private FileServiceClient fileServiceClient;

    @InjectMocks
    private ReadFileBolt readFileBolt;

    private StormTaskTuple stormTaskTuple;
    private static final String FILE_URL = "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2";

    @Before
    public void init() throws IllegalAccessException, MCSException, URISyntaxException {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
        stormTaskTuple = prepareTuple();
    }

    private final void verifyMethodExecutionNumber(int expectedCalls, int expectedEmitCallTimes, String file) throws MCSException, IOException {
        when(outputCollector.emit(anyList())).thenReturn(null);
        readFileBolt.execute(stormTaskTuple);
        verify(fileServiceClient, times(expectedCalls)).getFile(eq(file), eq(AUTHORIZATION), eq(AUTHORIZATION_HEADER));
        verify(outputCollector, times(expectedEmitCallTimes)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }


    @Test
    public void shouldEmmitNotificationWhenDataSetListHasOneElement() throws MCSException, IOException {
        //given
        when(fileServiceClient.getFile(eq(FILE_URL),eq(AUTHORIZATION), eq(AUTHORIZATION_HEADER))).thenReturn(null);
        verifyMethodExecutionNumber(1, 0, FILE_URL);
    }

    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingMCSException() throws MCSException, IOException {
        //given
        doThrow(MCSException.class).when(fileServiceClient).getFile(eq(FILE_URL),eq(AUTHORIZATION), eq(AUTHORIZATION_HEADER));
        verifyMethodExecutionNumber(11, 1, FILE_URL);
    }

    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingDriverException() throws MCSException, IOException {
        //given
        doThrow(DriverException.class).when(fileServiceClient).getFile(eq(FILE_URL),eq(AUTHORIZATION), eq(AUTHORIZATION_HEADER));
        verifyMethodExecutionNumber(11, 1, FILE_URL);
    }

    private StormTaskTuple prepareTuple() {
        stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION_HEADER);
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, FILE_URL);
        return stormTaskTuple;
    }


}