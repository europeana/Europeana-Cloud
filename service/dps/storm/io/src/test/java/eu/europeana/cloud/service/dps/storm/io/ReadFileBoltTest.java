package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/9/2018.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class ReadFileBoltTest {

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
    }

    private final void verifyMethodExecutionNumber(int expectedCalls, int expectedEmitCallTimes, List<String> files) throws MCSException, IOException {
        when(outputCollector.emit(anyList())).thenReturn(null);
        System.out.println(files.get(0));
        readFileBolt.emitFiles(fileServiceClient, stormTaskTuple, files);
        verify(fileServiceClient, times(expectedCalls)).getFile(eq(files.get(0)));
        verify(outputCollector, times(expectedEmitCallTimes)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME),anyListOf(Object.class));

    }


    @Test
    public void shouldEmmitNotificationWhenDataSetListHasOneElement() throws MCSException, IOException {
        //given
        List<String> files = prepareFiles();
        when(fileServiceClient.getFile(eq(files.get(0)))).thenReturn(null);
        stormTaskTuple = new StormTaskTuple();
        verifyMethodExecutionNumber(1, 0, files);
    }

    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingMCSException() throws MCSException, IOException {
        //given
        List<String> files = prepareFiles();
        doThrow(MCSException.class).when(fileServiceClient).getFile(eq(files.get(0)));
        stormTaskTuple = new StormTaskTuple();
        verifyMethodExecutionNumber(11, 1, files);
    }

    @Test
    public void shouldRetry10TimesBeforeFailingWhenThrowingDriverException() throws MCSException, IOException {
        //given
        List<String> files = prepareFiles();
        doThrow(DriverException.class).when(fileServiceClient).getFile(eq(files.get(0)));
        stormTaskTuple = new StormTaskTuple();
        verifyMethodExecutionNumber(11, 1, files);
    }

    private List<String> prepareFiles() {
        List<String> files = new ArrayList<>(1);
        files.add(FILE_URL);
        return files;
    }


}