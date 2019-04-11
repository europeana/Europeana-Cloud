package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 7/21/2017.
 */

public class WriteRecordBoltTest {

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    public static final String AUTHORIZATION = "Authorization";
    private final byte[] FILE_DATA = "Data".getBytes();

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "recordServiceClient")
    private RecordServiceClient recordServiceClient;


    @InjectMocks
    private WriteRecordBolt writeRecordBolt = new WriteRecordBolt("http://localhost:8080/mcs");

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void successfullyExecuteWriteBolt() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());
        when(outputCollector.emit(anyList())).thenReturn(null);
        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"))).thenReturn(uri);

        writeRecordBolt.execute(tuple);

        verify(outputCollector, times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(value.size(), 7);
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(parameters.get(PluginParameterKeys.OUTPUT_URL), SOURCE_VERSION_URL);

    }

    @Test
    public void shouldRetry3TimesBeforeFailingWhenThrowingMCSException() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());

        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION ,AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);


        doThrow(MCSException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString());
        writeRecordBolt.execute(tuple);
        verify(recordServiceClient, times(4)).getRepresentation(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"));
    }

    @Test
    public void shouldRetry3TimesBeforeFailingWhenThrowingDriverException() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());

        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);


        doThrow(DriverException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString());
        writeRecordBolt.execute(tuple);
        verify(recordServiceClient, times(4)).getRepresentation(eq(SOURCE + CLOUD_ID), eq(SOURCE + REPRESENTATION_NAME), eq(SOURCE + VERSION), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"));
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.CLOUD_ID, SOURCE + CLOUD_ID);
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        parameters.put(PluginParameterKeys.REPRESENTATION_VERSION, SOURCE + VERSION);
        return parameters;
    }

}