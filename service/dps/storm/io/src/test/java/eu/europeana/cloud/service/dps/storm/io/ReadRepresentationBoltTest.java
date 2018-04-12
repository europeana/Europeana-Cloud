package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.io.ReadRepresentationBolt.getTestInstance;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskStatusChecker.class})
public class ReadRepresentationBoltTest {

    private ReadRepresentationBolt instance;
    private OutputCollector oc;

    private static TaskStatusChecker taskStatusChecker;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = null;
    private FileServiceClient fileClient;
    private TestHelper testHelper;

    @BeforeClass
    public static void initTaskStatusChecker() throws Exception {
        taskStatusChecker = Mockito.mock(TaskStatusChecker.class);
        PowerMockito.mockStatic(TaskStatusChecker.class);
        when(TaskStatusChecker.getTaskStatusChecker()).thenReturn(taskStatusChecker);
    }

    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        fileClient = mock(FileServiceClient.class);
        instance = getTestInstance("http://localhost:8080/mcs", oc, taskStatusChecker);
        testHelper = new TestHelper();
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representation), new Revision());
        when(fileClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, false);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        String exptectedFileUrl = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/fileName";
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        for (Values values : allValues) {
            assertNotNull(values);
            assertTrue(values.size() >= 4);
            assertFile(exptectedFileUrl, values);
        }
        verifyNoMoreInteractions(oc);
    }

    @Test
    public void NoFilesShouldBeEmittedIfTaskWasKilled() throws MCSException, URISyntaxException {
        //given
        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representation), new Revision());
        when(fileClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, true);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        String exptectedFileUrl = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/fileName";
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        for (Values values : allValues) {
            assertNotNull(values);
            assertTrue(values.size() >= 4);
            assertFile(exptectedFileUrl, values);
        }
        verifyNoMoreInteractions(oc);
    }


    private void assertFile(String expectedFileUrl, Values values) {
        String fileUrl = ((Map<String, String>) values.get(4)).get(PluginParameterKeys.DPS_TASK_INPUT_DATA);
        assertThat(fileUrl, is(expectedFileUrl));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(Representation representation) {
        HashMap<String, String> parameters = new HashMap<>();
        String RepresentationsJson = new Gson().toJson(representation);
        parameters.put(PluginParameterKeys.REPRESENTATION, RepresentationsJson);
        return parameters;
    }
}