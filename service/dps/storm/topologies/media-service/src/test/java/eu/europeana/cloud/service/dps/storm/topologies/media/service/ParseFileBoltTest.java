package eu.europeana.cloud.service.dps.storm.topologies.media.service;


import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

/**
 * Created by Tarek on 12/6/2018.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "org.apache.logging.log4j.*", "javax.xml.*", "org.xml.sax.*", "org.w3c.dom.*"})
public class ParseFileBoltTest {

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
    static ParseFileBolt parseFileBolt = new ParseFileBolt("localhost/mcs");

    private StormTaskTuple stormTaskTuple;
    private static List<String> expectedParametersKeysList;

    @BeforeClass
    public static void init() {

        parseFileBolt.prepare();
        expectedParametersKeysList = Arrays.asList(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.RESOURCE_LINK_KEY, PluginParameterKeys.DPS_TASK_INPUT_DATA, PluginParameterKeys.RESOURCE_LINKS_COUNT);

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
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION);
        setStaticField(ParseFileBolt.class.getSuperclass().getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
    }

    @Test
    public void shouldParseFileAndEmitResources() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            when(taskStatusChecker.hasKillFlag(eq(TASK_ID))).thenReturn(false);
            parseFileBolt.execute(stormTaskTuple);
            verify(outputCollector, Mockito.times(5)).emit(captor.capture()); // 4 hasView, 1 edm:object

            List<Values> capturedValuesList = captor.getAllValues();
            assertEquals(5, capturedValuesList.size());
            for (Values values : capturedValuesList) {
                assertEquals(7, values.size());
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
        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            when(taskStatusChecker.hasKillFlag(eq(TASK_ID))).thenReturn(false).thenReturn(false).thenReturn(true);
            parseFileBolt.execute(stormTaskTuple);
            verify(outputCollector, Mockito.times(2)).emit(captor.capture()); // 4 hasView, 1 edm:object, dropped after 2 resources
        }
    }

    @Test
    public void shouldParseFileWithEmptyResourcesAndForwardOneTuple() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/no-resources.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            parseFileBolt.execute(stormTaskTuple);
            verify(outputCollector, Mockito.times(1)).emit(captor.capture());
            Values values = captor.getValue();
            assertNotNull(values);
            System.out.println(values);
            Map<String, String> map = (Map) values.get(4);
            System.out.println(map);
            assertEquals(2, map.size());
            assertNull(map.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
            assertNull(map.get(PluginParameterKeys.RESOURCE_LINK_KEY));
        }

    }

    @Test
    public void shouldEmitErrorWhenDownloadFileFails() throws Exception {
        doThrow(IOException.class).when(fileClient).getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION));
        parseFileBolt.execute(stormTaskTuple);
        verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), captor.capture());
        Values values = captor.getValue();
        assertNotNull(values);
        Map<String, String> valueMap = (Map) values.get(2);
        assertNotNull(valueMap);
        assertEquals(4, valueMap.size());
        assertTrue(valueMap.get("additionalInfo").contains("Error while reading and parsing the EDM file"));
        assertEquals(States.ERROR.toString(), valueMap.get("state"));
        assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        verify(outputCollector, Mockito.times(0)).emit(anyList());
    }


    @Test
    public void shouldEmitErrorWhenGettingResourceLinksFails() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/broken.xml")) {
            when(fileClient.getFile(eq(FILE_URL), eq(AUTHORIZATION), eq(AUTHORIZATION))).thenReturn(stream);
            parseFileBolt.execute(stormTaskTuple);
            verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), captor.capture());
            Values values = captor.getValue();
            assertNotNull(values);
            Map<String, String> valueMap = (Map) values.get(2);
            assertNotNull(valueMap);
            assertEquals(4, valueMap.size());
            assertTrue(valueMap.get("additionalInfo").contains("Error while reading and parsing the EDM file"));
            assertEquals(States.ERROR.toString(), valueMap.get("state"));
            assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
            verify(outputCollector, Mockito.times(0)).emit(anyList());
        }

    }
}