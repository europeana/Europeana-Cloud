package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 7/21/2017.
 */

public class WriteRecordBoltTest {

    public static final String AUTHORIZATION = "Authorization";
    private static final String SENT_DATE = "2021-07-16T10:40:02.351Z";
    private static final UUID NEW_VERSION = UUID.fromString("2d04fbf0-e622-11eb-8000-88029720479f");
    private static final String NEW_FILE_NAME = "0e7b8802-9720-379f-9abb-672abfa81076";
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final byte[] FILE_DATA = "Data".getBytes();

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
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
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());
        when(outputCollector.emit(anyList())).thenReturn(null);
        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(any(), any(), any(), any(), any(InputStream.class), any(), any(), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"))).thenReturn(uri);

        writeRecordBolt.execute(anchorTuple, tuple);

        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(8, value.size());
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
        verify(recordServiceClient).createRepresentation(any(), any(), any(), eq(NEW_VERSION), any(InputStream.class),
                eq(NEW_FILE_NAME), any(), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"));


    }

    @Test
    public void successfullyExecuteWriteBoltOnDeletedRecord() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());
        tuple.addParameter(PluginParameterKeys.MARKED_AS_DELETED,"true");
        when(outputCollector.emit(anyList())).thenReturn(null);
        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(any(), any(), any(), (UUID)any(), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"))).thenReturn(uri);

        writeRecordBolt.execute(anchorTuple, tuple);

        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(8, value.size());
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
        verify(recordServiceClient).createRepresentation(any(), any(), any(), eq(NEW_VERSION), eq(AUTHORIZATION), eq("AUTHORIZATION_HEADER"));
    }

    @Test
    public void shouldRetry7TimesBeforeFailingWhenThrowingMCSException() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());

        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);


        doThrow(MCSException.class).when(recordServiceClient).createRepresentation(any(), any(), any(), any(), any(InputStream.class), any(), any(), any(), any());
        writeRecordBolt.execute(anchorTuple, tuple);
        verify(recordServiceClient, times(8)).createRepresentation(any(), any(), any(), any(), any(InputStream.class), any(), any(), any(), any());
    }

    @Test
    public void shouldRetry7TimesBeforeFailingWhenThrowingDriverException() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision());

        Representation representation = mock(Representation.class);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, "AUTHORIZATION_HEADER")).thenReturn(representation);
        when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);


        doThrow(DriverException.class).when(recordServiceClient).createRepresentation(any(), any(), any(), any(), any(InputStream.class), any(), any(), any(), any());
        writeRecordBolt.execute(anchorTuple, tuple);
        verify(recordServiceClient, times(8)).createRepresentation(any(), any(), any(), any(), any(InputStream.class), any(), any(), any(), any());
    }

    private HashMap<String, String> prepareStormTaskTupleParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.CLOUD_ID, SOURCE + CLOUD_ID);
        parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        parameters.put(PluginParameterKeys.REPRESENTATION_VERSION, SOURCE + VERSION);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        parameters.put(PluginParameterKeys.SENT_DATE, SENT_DATE);
        return parameters;
    }

}