package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;


public class ParseFileBoltTest {
    private static final long serialVersionUID = 1L;

    private static final String AUTHORIZATION = "Authorization";
    private static final String FILE_URL = "FILE_URL";
    private static final String NOTIFICATION_STREAM_NAME = "NotificationStream";
    private static final long TASK_ID = 1;


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "fileClient")
    private FileServiceClient fileClient;

    @Mock(name = "taskStatusChecker")
    private TaskStatusChecker taskStatusChecker;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


    @InjectMocks
    static ParseFileForMediaBolt parseFileBolt = new ParseFileForMediaBolt("localhost/mcs");

    private StormTaskTuple stormTaskTuple;
    private static List<String> expectedParametersKeysList;

    @BeforeClass
    public static void init() {

        parseFileBolt.prepare();
        expectedParametersKeysList = Arrays.asList(
                PluginParameterKeys.AUTHORIZATION_HEADER,
                PluginParameterKeys.RESOURCE_LINK_KEY,
                PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER,
                PluginParameterKeys.RESOURCE_URL,
                PluginParameterKeys.RESOURCE_LINKS_COUNT,
                PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE,
                PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS,
                PluginParameterKeys.RESOURCE_LINK_KEY);

    }

    void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }


    @Before
    public void prepareTuple() throws Exception {
        MockitoAnnotations.initMocks(this);
        stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.setTaskId(TASK_ID);
        stormTaskTuple.setFileUrl(FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION);
        stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, "3");
//        setStaticField(ParseFileForMediaBolt.class.getSuperclass().getSuperclass().getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
    }

    @Test
    public void shouldParseFileAndEmitResources() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);

        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            when(taskStatusChecker.hasDroppedStatus(eq(TASK_ID))).thenReturn(false);
            parseFileBolt.execute(anchorTuple, stormTaskTuple);
            verify(outputCollector, Mockito.times(4)).emit(any(Tuple.class), captor.capture()); // 4 hasView, 1 edm:object

            List<Values> capturedValuesList = captor.getAllValues();
            assertEquals(4, capturedValuesList.size());
            for (Values values : capturedValuesList) {
                assertEquals(8, values.size());
                Map<String, String> val = (Map) values.get(4);
                assertNotNull(val);
                for (String parameterKey : val.keySet()) {
                    assertTrue(expectedParametersKeysList.contains(parameterKey));
                }
            }
        }
    }


    @Test
    public void shouldDropTaskAndStopEmitting() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);

        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            when(taskStatusChecker.hasDroppedStatus(eq(TASK_ID))).thenReturn(false).thenReturn(false).thenReturn(true);
            parseFileBolt.execute(anchorTuple, stormTaskTuple);
            verify(outputCollector, Mockito.times(2)).emit(any(Tuple.class), captor.capture()); // 4 hasView, 1 edm:object, dropped after 2 resources
        }
    }

    @Test
    public void shouldParseFileWithEmptyResourcesAndForwardOneTuple() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, "0");

        try (InputStream stream = this.getClass().getResourceAsStream("/files/no-resources.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            parseFileBolt.execute(anchorTuple, stormTaskTuple);
            verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
            Values values = captor.getValue();
            assertNotNull(values);
            System.out.println(values);
            Map<String, String> map = (Map) values.get(4);
            System.out.println(map);
            assertEquals(4, map.size());
            assertNotNull(map.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
            assertNull(map.get(PluginParameterKeys.RESOURCE_LINK_KEY));
        }

    }

    @Test
    public void shouldEmitErrorWhenDownloadFileFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        doThrow(MCSException.class).when(fileClient).getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION));
        parseFileBolt.execute(anchorTuple, stormTaskTuple);
        verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
        Values values = captor.getValue();
        assertNotNull(values);
        Map<String, String> valueMap = (Map) values.get(1);
        assertNotNull(valueMap);
        assertEquals(5, valueMap.size());
        assertTrue(valueMap.get("additionalInfo").contains("Error while reading and parsing the EDM file"));
        assertEquals(RecordState.ERROR.toString(), valueMap.get("state"));
        assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        verify(outputCollector, Mockito.times(0)).emit(anyList());
    }


    @Test
    public void shouldEmitErrorWhenGettingResourceLinksFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        try (InputStream stream = this.getClass().getResourceAsStream("/files/broken.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            parseFileBolt.execute(anchorTuple, stormTaskTuple);
            verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
            Values values = captor.getValue();
            assertNotNull(values);
            Map<String, String> valueMap = (Map) values.get(1);
            assertNotNull(valueMap);
            assertEquals(5, valueMap.size());
            assertTrue(valueMap.get("additionalInfo").contains("Error while reading and parsing the EDM file"));
            assertEquals(RecordState.ERROR.toString(), valueMap.get("state"));
            assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
            verify(outputCollector, Mockito.times(0)).emit(any(Tuple.class), anyList());
        }

    }
}
