package eu.europeana.cloud.service.dps.storm.io;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.REVISION_PROVIDER;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.REVISION_TIMESTAMP;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WriteRecordBoltTest {

  private static final String SENT_DATE = "2021-07-16T10:40:02.351Z";
  private static final UUID NEW_VERSION = UUID.fromString("2d04fbf0-e622-11eb-8000-8c50aca96d65");
  private static final String NEW_FILE_NAME = "0e7b8802-9720-379f-9abb-672abfa81076";
  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";
  private final byte[] FILE_DATA = "Data".getBytes();
  private final int retryAttemptsCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(8);

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;
  @Mock(name = "recordServiceClient")
  private RecordServiceClient recordServiceClient;
  @InjectMocks
  private WriteRecordBolt writeRecordBolt =
      new WriteRecordBolt(new CassandraProperties(), "http://localhost:8080/mcs", "user", "password");

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void successfullyExecuteWriteBolt() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters());
    when(outputCollector.emit(anyList())).thenReturn(null);
    Representation representation = mock(Representation.class);
    when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(
        representation);
    when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(any(), any(), any(), any(), any(), any(InputStream.class), any(),
        any())).thenReturn(uri);

    writeRecordBolt.execute(anchorTuple, tuple);

    verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    Values value = captor.getAllValues().get(0);
    assertEquals(10, value.size());
    assertTrue(value.get(4) instanceof Map);
    Map<String, String> parameters = (Map<String, String>) value.get(4);
    assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
    assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
    verify(recordServiceClient).createRepresentation(any(), any(), any(), eq(NEW_VERSION), eq(DATASET_NAME),
        any(InputStream.class), eq(NEW_FILE_NAME), any());


  }

  @Test
  public void successfullyExecuteWriteBoltOnDeletedRecord() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters());
    tuple.addParameter(PluginParameterKeys.MARKED_AS_DELETED, "true");
    when(outputCollector.emit(anyList())).thenReturn(null);
    Representation representation = mock(Representation.class);
    when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(
        representation);
    when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(any(), any(), any(), any(), anyString(), anyBoolean())).thenReturn(uri);

    writeRecordBolt.execute(anchorTuple, tuple);

    verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    Values value = captor.getAllValues().get(0);
    assertEquals(10, value.size());
    assertTrue(value.get(4) instanceof Map);
    Map<String, String> parameters = (Map<String, String>) value.get(4);
    assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
    assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
    verify(recordServiceClient).createRepresentation(any(), any(), any(), eq(NEW_VERSION), anyString(), anyBoolean());
  }

  @Test
  public void successfullyExecuteWriteBoltOnDeletedRecordWithRevisionOrientedProcessing() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
            prepareStormTaskTupleParametersForRevisionOrientedProcessing(), new Revision());
    tuple.addParameter(PluginParameterKeys.MARKED_AS_DELETED, "true");
    when(outputCollector.emit(anyList())).thenReturn(null);
    Representation representation = mock(Representation.class);
    when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(
            representation);
    when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(any(), any(), any(), any(), anyString(), anyBoolean())).thenReturn(uri);

    writeRecordBolt.execute(anchorTuple, tuple);

    verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    Values value = captor.getAllValues().get(0);
    assertEquals(10, value.size());
    assertTrue(value.get(4) instanceof Map);
    Map<String, String> parameters = (Map<String, String>) value.get(4);
    assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
    assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
    verify(recordServiceClient).createRepresentation(any(), any(), any(), eq(NEW_VERSION), anyString(), anyBoolean());
  }

  @Test
  public void shouldRetryBeforeFailingWhenThrowingMCSException() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), new Revision());

    Representation representation = mock(Representation.class);
    when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(
        representation);
    when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);

    doThrow(MCSException.class).when(recordServiceClient)
                               .createRepresentation(any(), any(), any(), any(), any(), any(InputStream.class), any(), any());
    writeRecordBolt.execute(anchorTuple, tuple);
    verify(recordServiceClient, times(retryAttemptsCount)).createRepresentation(any(), any(), any(), any(), any(),
        any(InputStream.class), any(),
        any());
  }

  @Test
  public void shouldRetryBeforeFailingWhenThrowingDriverException() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), new Revision());

    Representation representation = mock(Representation.class);
    when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(
        representation);
    when(representation.getDataProvider()).thenReturn(DATA_PROVIDER);

    doThrow(DriverException.class).when(recordServiceClient)
                                  .createRepresentation(any(), any(), any(), any(), any(), any(InputStream.class), any(), any());
    writeRecordBolt.execute(anchorTuple, tuple);
    verify(recordServiceClient, times(retryAttemptsCount)).createRepresentation(any(), any(), any(), any(), any(),
        any(InputStream.class), any(),
        any());
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.CLOUD_ID, SOURCE + CLOUD_ID);
    parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
    parameters.put(PluginParameterKeys.REPRESENTATION_VERSION, SOURCE + VERSION);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    parameters.put(PluginParameterKeys.OUTPUT_DATA_SETS,
        "https://127.0.0.1:8080/mcs/data-providers/exampleProvider/data-sets/dataSet");
    parameters.put(PluginParameterKeys.SENT_DATE, SENT_DATE);
    parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA,
            "https://127.0.0.1:8080/mcs/data-providers/exampleProvider/data-sets/inputDataSet");
    return parameters;
  }

  private HashMap<String, String> prepareStormTaskTupleParametersForRevisionOrientedProcessing() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.CLOUD_ID, SOURCE + CLOUD_ID);
    parameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
    parameters.put(PluginParameterKeys.REPRESENTATION_VERSION, SOURCE + VERSION);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    parameters.put(PluginParameterKeys.REVISION_NAME, REVISION_NAME);
    parameters.put(PluginParameterKeys.REVISION_PROVIDER, REVISION_PROVIDER);
    parameters.put(PluginParameterKeys.REVISION_TIMESTAMP, REVISION_TIMESTAMP);
    parameters.put(PluginParameterKeys.OUTPUT_DATA_SETS,
            "https://127.0.0.1:8080/mcs/data-providers/exampleProvider/data-sets/dataSet");
    parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA,
            "https://127.0.0.1:8080/mcs/data-providers/exampleProvider/data-sets/dataSet");
    parameters.put(PluginParameterKeys.SENT_DATE, SENT_DATE);
    return parameters;
  }

}