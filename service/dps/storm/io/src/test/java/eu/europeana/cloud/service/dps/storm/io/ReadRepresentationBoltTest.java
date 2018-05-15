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
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadRepresentationBolt.class})
public class ReadRepresentationBoltTest {


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "fileServiceClient")
    private FileServiceClient fileServiceClient;

    @Mock(name = "taskStatusChecker")
    private TaskStatusChecker taskStatusChecker;

    @InjectMocks
    private ReadRepresentationBolt instance;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
    private final byte[] FILE_DATA = null;

    private TestHelper testHelper = new TestHelper();


    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
        mockStaticField(ReadRepresentationBolt.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
    }

    private static void mockStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void successfulExecuteStormTuple() throws MCSException, URISyntaxException {
        //given
        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representation), new Revision());
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, false);
        when(outputCollector.emit(anyList())).thenReturn(null);
       //when
        instance.readRepresentationBolt(fileServiceClient, tuple);
        //then

        verify(outputCollector, times(2)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        List<Values> allValues = captor.getAllValues();
        for (Values values : allValues) {
            assertNotNull(values);
            assertTrue(values.size() >= 4);
            assertFile(FILE_URL, values);
        }
    }


    @Test
    public void shouldRetry10TimesBeforeFailing() throws MCSException, URISyntaxException {
        //given
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representation), new Revision());
        doThrow(MCSException.class).when(fileServiceClient).getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"));
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        when(outputCollector.emit(eq(NOTIFICATION_STREAM_NAME), anyList())).thenReturn(null);
        //when
        instance.readRepresentationBolt(fileServiceClient, tuple);
        //then
        verify(outputCollector, times(0)).emit(anyList());
    }


    @Test
    public void NoFilesShouldBeEmittedIfTaskWasKilled() throws MCSException, URISyntaxException {
        //given
        Representation representation = testHelper.prepareRepresentationWithMultipleFiles(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date(), 2);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, FILE_DATA, prepareStormTaskTupleParameters(representation), new Revision());
        when(fileServiceClient.getFileUri(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq("fileName"))).thenReturn(new URI(FILE_URL)).thenReturn(new URI(FILE_URL));
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, true);
        when(outputCollector.emit(anyList())).thenReturn(null);

        //when
        instance.readRepresentationBolt(fileServiceClient, tuple);
        verify(outputCollector, times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        for (Values values : allValues) {
            assertNotNull(values);
            assertTrue(values.size() >= 4);
            assertFile(FILE_URL, values);
        }
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