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
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
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
import java.net.MalformedURLException;
import java.net.URI;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by Tarek on 7/21/2017.
 */
public class HarvestingWriteRecordBoltTest {

    private static final String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private static final String AUTHORIZATION = "Authorization";
    private final byte[] FILE_DATA = "Data".getBytes();
    private OAIPMHHarvestingDetails oaipmhHarvestingDetails;

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "recordServiceClient")
    private RecordServiceClient recordServiceClient;

    @Mock(name = "uisClient")
    private UISClient uisClient;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @InjectMocks
    private HarvestingWriteRecordBolt oaiWriteRecordBolt = new HarvestingWriteRecordBolt("http://localhost:8080/mcs", "http://localhost:8080/uis");

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
        when(outputCollector.emit(anyList())).thenReturn(null);
        MockitoAnnotations.initMocks(this);
        oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<String>(Arrays.asList(SOURCE + REPRESENTATION_NAME)));
    }

    private StormTaskTuple getStormTaskTuple() throws Exception {
        return new StormTaskTuple(
                TASK_ID,
                TASK_NAME,
                SOURCE_VERSION_URL,
                FILE_DATA,
                prepareStormTaskTupleParameters(),
                new Revision(),
                oaipmhHarvestingDetails,
                0,
                Arrays.asList(SOURCE + LOCAL_ID, "id2", "id3"));
    }

    private StormTaskTuple stormTaskTupleWithoutLocalIdentifiers() throws Exception {
        return new StormTaskTuple(
                TASK_ID,
                TASK_NAME,
                SOURCE_VERSION_URL,
                FILE_DATA,
                prepareStormTaskTupleParameters(),
                new Revision(),
                oaipmhHarvestingDetails,
                0,
                Arrays.asList());
    }

    private StormTaskTuple getStormTaskTupleWithAdditionalLocalIdParam() throws Exception {
        return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
                prepareStormTaskTupleParameters(), new Revision(),
                oaipmhHarvestingDetails, 0,
                Arrays.asList(SOURCE + LOCAL_ID, "id2", "id3"));
    }

    private StormTaskTuple stormTaskTupleWithOneAdditionalLocalIdParam() throws Exception {
        return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
                prepareStormTaskTupleParameters(), new Revision(),
                oaipmhHarvestingDetails, 0,
                Arrays.asList(SOURCE + LOCAL_ID, "id2"));
    }

    @Test
    public void successfulExecuteStormTupleWithExistedCloudId() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTuple());

        assertExecutionResults();
        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(0)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(2)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), anyString(), anyString(), anyString());
    }

    @Test
    public void successfulEmitErrorNotificationInCaseOfMissingLocalIdentifiers() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBolt.execute(anchorTuple, stormTaskTupleWithoutLocalIdentifiers());
        //
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verify(outputCollector, times(0)).emit(any(Tuple.class), captor.capture());
        //
        verify(uisClient, times(0)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(0)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(0)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), anyString(), anyString(), anyString());
    }

    @Test
    public void successfulExecuteStormTupleWithDeletedRecord() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(uri);

        StormTaskTuple stormTaskTuple = getStormTaskTuple();
        stormTaskTuple.setMarkedAsDeleted(true);
        oaiWriteRecordBolt.execute(anchorTuple, stormTaskTuple);

        assertExecutionResults();
    }

    @Test
    public void shouldRetry7TimesBeforeFailingWhenThrowingMCSException() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        doThrow(MCSException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString());
        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTuple());
        assertFailingExpectationWhenCreatingRepresentation();
    }

    @Test
    public void shouldRetry7TimesBeforeFailingWhenThrowingDriverException() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        doThrow(DriverException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString());
        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTuple());
        assertFailingExpectationWhenCreatingRepresentation();
    }

    private void assertFailingExpectationWhenCreatingRepresentation() throws MCSException, IOException {
        verify(outputCollector, times(0)).emit(anyList());
        verify(recordServiceClient, times(8)).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyListOf(Object.class));
    }

    @Test
    public void successfulExecuteStormTupleWithCreatingNewCloudId() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTuple());

        assertExecutionResults();

        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(1)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(2)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), anyString(), anyString(), anyString());

    }

    @Test
    public void shouldSuccessfullyExecuteStormTupleWithCreatingNewCloudIdAndAdditionalLocalIdMappings() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier",AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(true);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

        assertExecutionResults();

        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(1)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(2)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), anyString(), anyString(), anyString());
    }

    @Test
    public void shouldSuccessfullyExecuteStormTupleWithCreatingNewCloudIdAndAdditionalOneMapping() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier",AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(true);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBolt.execute(anchorTuple, stormTaskTupleWithOneAdditionalLocalIdParam());

        assertExecutionResults();

        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(1)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(1)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), anyString(), anyString(), anyString());
    }

    @Test
    public void shouldSuccessfullyExecuteStormTupleWhenAdditionalMappingAlreadyExist() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        CloudException idHasBeenMappedException = new CloudException("", new IdHasBeenMappedException(new ErrorInfo()));
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "id2")).thenThrow(idHasBeenMappedException);
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "id3")).thenThrow(idHasBeenMappedException);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), any(), anyString(),anyString(),anyString())).thenReturn(uri);

        //when
        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

        //then
        assertExecutionResults();

        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(1)).createCloudId(anyString(), anyString(), anyString(), anyString());
        verify(uisClient, times(1)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), eq("id2"), anyString(), anyString());
        verify(uisClient, times(1)).createMapping(anyString(), eq(SOURCE + DATA_PROVIDER), eq("id3"), anyString(), anyString());
    }

    @Test
    public void shouldRetry7BeforeFailingWhenMappingAdditionalLocalId() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudId cloudId = mock(CloudId.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);

        when(uisClient.createMapping(eq(cloudId.getId()), eq(SOURCE + DATA_PROVIDER), anyString(), eq(AUTHORIZATION), eq(AUTHORIZATION_HEADER))).thenThrow(CloudException.class);
        //when
        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTupleWithAdditionalLocalIdParam());

        //then
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(1)).getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(1)).createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        verify(uisClient, times(8)).createMapping(isNull(), anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyListOf(Object.class));

    }

    @Test
    public void shouldRetry7TimesBeforeFailingWhenCreatingNewCloudId() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        doThrow(CloudException.class).when(uisClient).createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        oaiWriteRecordBolt.execute(anchorTuple, getStormTaskTuple());
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(8)).createCloudId(anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyListOf(Object.class));

    }

    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION_HEADER);
        parameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE + LOCAL_ID);
        parameters.put(PluginParameterKeys.PROVIDER_ID, SOURCE + DATA_PROVIDER);
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");
        return parameters;
    }

    private void assertExecutionResults() {
        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(9, value.size());
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(SOURCE_VERSION_URL, parameters.get(PluginParameterKeys.OUTPUT_URL));
    }
}

