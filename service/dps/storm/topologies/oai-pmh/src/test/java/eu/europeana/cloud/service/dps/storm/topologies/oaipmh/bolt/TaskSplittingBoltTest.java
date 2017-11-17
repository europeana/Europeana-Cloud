package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by Tarek on 11/9/2017.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TaskSplittingBolt.class)
public class TaskSplittingBoltTest {
    @Mock
    private OutputCollector outputCollector;

    private static long INTERVAL = 2592000000l;
    @InjectMocks
    private TaskSplittingBolt taskSplittingBolt = new TaskSplittingBolt(INTERVAL);

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private static final int TASK_ID = 1;
    private static final String SOURCE_URL = "http://sourceFileURl.com";
    private static final String TASK_NAME = "TASK_NAME";

    private static StormTaskTuple tuple;
    private static Splitter splitter;


    @BeforeClass
    public static void initialize() throws Exception {
        //given
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, SOURCE_URL);
        tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_URL, null, parameters, null);
        splitter = mock(Splitter.class);
        whenNew(Splitter.class).withAnyArguments().thenReturn(splitter);

    }

    @Test
    public void testNormalExecution() throws Exception {
        doNothing().when(splitter).splitBySchema();
        taskSplittingBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
    }

    @Test
    public void testExceptionExecution() throws Exception {
        doThrow(new RuntimeException()).when(splitter).splitBySchema();
        taskSplittingBolt.execute(tuple);
        verify(outputCollector, times(1)).emit(eq("NotificationStream"), Mockito.anyList());
    }

}