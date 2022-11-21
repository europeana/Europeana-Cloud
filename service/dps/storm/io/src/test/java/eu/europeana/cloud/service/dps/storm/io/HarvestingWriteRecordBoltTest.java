package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HarvestingWriteRecordBoltTest {

  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";
  private final byte[] FILE_DATA = "Data".getBytes();
  private static final String SENT_DATE = "2021-07-16T10:40:02.351Z";
  private OAIPMHHarvestingDetails oaipmhHarvestingDetails;

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "recordServiceClient")
  private RecordServiceClient recordServiceClient;

  @Mock(name = "uisClient")
  private UISClient uisClient;

  @Captor
  ArgumentCaptor<Values> captor; /*= ArgumentCaptor.forClass(Values.class);*/

  @InjectMocks
  private HarvestingWriteRecordBolt oaiWriteRecordBoltT = new HarvestingWriteRecordBolt(
      "http://localhost:8080/mcs",
      "http://localhost:8080/uis",
      "user",
      "password");

  @Before
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    when(outputCollector.emit(anyList())).thenReturn(null);
    MockitoAnnotations.initMocks(this);
    oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
    oaipmhHarvestingDetails.setSchema(SOURCE + REPRESENTATION_NAME);
  }

  private StormTaskTuple getStormTaskTuple() {
    return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(),
        new Revision(), oaipmhHarvestingDetails);
  }

  private StormTaskTuple getStormTaskTupleWithAdditionalLocalIdParam() {
    HashMap<String, String> parameters = prepareStormTaskTupleParameters();
    parameters.put(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, "additionalLocalIdentifier");
    return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, parameters, new Revision(),
        oaipmhHarvestingDetails);
  }


  @Test
  public void successfulExecuteStormTupleWithExistedCloudId() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(), any(), any(InputStream.class),
        any(), anyString())).thenReturn(uri);

    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTuple());

    assertExecutionResults();

  }

  @Test
  public void successfulExecuteStormTupleWithDeletedRecord() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(UUID.class), any())).thenReturn(uri);

    StormTaskTuple stormTaskTuple = getStormTaskTuple();
    stormTaskTuple.setMarkedAsDeleted(true);
    oaiWriteRecordBoltT.execute(anchorTuple, stormTaskTuple);

    assertExecutionResults();

  }

  @Test
  public void shouldRetry7TimesBeforeFailingWhenThrowingMCSException() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    doThrow(MCSException.class).when(recordServiceClient)
                               .createRepresentation(anyString(), anyString(), anyString(), any(), any(), any(InputStream.class),
                                   any(), anyString());
    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTuple());
    assertFailingExpectationWhenCreatingRepresentation();
  }

  @Test
  public void shouldRetry7TimesBeforeFailingWhenThrowingDriverException() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    doThrow(DriverException.class).when(recordServiceClient)
                                  .createRepresentation(anyString(), anyString(), anyString(), any(), any(),
                                      any(InputStream.class), anyString(), anyString());
    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTuple());
    assertFailingExpectationWhenCreatingRepresentation();
  }

  private void assertFailingExpectationWhenCreatingRepresentation() throws MCSException, IOException {
    verify(outputCollector, times(0)).emit(anyList());
    verify(recordServiceClient, times(8)).createRepresentation(anyString(), anyString(), anyString(), any(), any(),
        any(InputStream.class), any(), anyString());
    verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());

  }

  @Test
  public void successfulExecuteStormTupleWithCreatingNewCloudId() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenThrow(exception);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(), any(), any(InputStream.class),
        any(), anyString())).thenReturn(uri);

    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTuple());

    assertExecutionResults();


  }

  @Test
  public void shouldSuccessfullyExecuteStormTupleWithCreatingNewCloudIdAndAdditionalLocalIdMapping() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenThrow(exception);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier")).thenReturn(true);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(), any(), any(InputStream.class),
        any(), anyString())).thenReturn(uri);

    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

    assertExecutionResults();

  }

  @Test
  public void shouldSuccessfullyExecuteStormTupleWhenAdditionalMappingAlreadyExist() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenThrow(exception);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
    URI uri = new URI(SOURCE_VERSION_URL);
    when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(), any(), any(InputStream.class),
        any(), anyString())).thenReturn(uri);

    //when
    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

    //then
    assertExecutionResults();
  }

  @Test
  public void shouldRetry7BeforeFailingWhenMappingAdditionalLocalId() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudId cloudId = mock(CloudId.class);
    when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
    when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);

    when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier")).thenThrow(
        CloudException.class);
    //when
    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

    //then
    verify(outputCollector, times(0)).emit(anyList());
    verify(uisClient, times(8)).createMapping(anyString(), anyString(), anyString());
    verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());

  }

  @Test
  public void shouldRetry7TimesBeforeFailingWhenCreatingNewCloudId() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
    when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenThrow(exception);
    doThrow(CloudException.class).when(uisClient).createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID);
    oaiWriteRecordBoltT.execute(anchorTuple, getStormTaskTuple());
    verify(outputCollector, times(0)).emit(anyList());
    verify(uisClient, times(8)).createCloudId(anyString(), anyString());
    verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());

  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE + LOCAL_ID);
    parameters.put(PluginParameterKeys.PROVIDER_ID, SOURCE + DATA_PROVIDER);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");
    parameters.put(PluginParameterKeys.OUTPUT_DATA_SETS,
        "https://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/sampleDataset");
    parameters.put(PluginParameterKeys.SENT_DATE, SENT_DATE);
    return parameters;
  }

  private void assertExecutionResults() {
    verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    Values value = captor.getAllValues().get(0);
    assertEquals(9, value.size());
    assertTrue(value.get(4) instanceof Map);
    var parameters = (Map<?, ?>) value.get(4);
    assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
    assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
  }
}

