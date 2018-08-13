package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
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
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
@RunWith(PowerMockRunner.class)
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


    @InjectMocks
    private HarvestingWriteRecordBolt oaiWriteRecordBoltT = new HarvestingWriteRecordBolt("http://localhost:8080/mcs", "http://localhost:8080/uis");

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
        when(outputCollector.emit(anyList())).thenReturn(null);
        MockitoAnnotations.initMocks(this);
        oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<String>(Arrays.asList(SOURCE + REPRESENTATION_NAME)));
    }

    private StormTaskTuple getStormTaskTuple() throws Exception {
        return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision(), oaipmhHarvestingDetails);
    }

    private StormTaskTuple getStormTaskTupleWithAdditionalLocalIdParam() throws Exception {
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, "additionalLocalIdentifier");
        return new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, parameters, new Revision(), oaipmhHarvestingDetails);
    }


    @Test
    public void successfulExecuteStormTupleWithExistedCloudId() throws Exception {
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBoltT.execute(getStormTaskTuple());

        assertExecutionResults();

    }

    @Test
    public void shouldRetry3TimesBeforeFailingWhenThrowingMCSException() throws Exception {
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        doThrow(MCSException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString());
        oaiWriteRecordBoltT.execute(getStormTaskTuple());
        assertFailingExpectationWhenCreatingRepresentation();
    }

    @Test
    public void shouldRetry3TimesBeforeFailingWhenThrowingDriverException() throws Exception {
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        doThrow(DriverException.class).when(recordServiceClient).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString());
        oaiWriteRecordBoltT.execute(getStormTaskTuple());
        assertFailingExpectationWhenCreatingRepresentation();
    }

    private void assertFailingExpectationWhenCreatingRepresentation() throws MCSException, IOException {
        verify(outputCollector, times(0)).emit(anyList());
        verify(recordServiceClient, times(4)).createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void successfulExecuteStormTupleWithCreatingNewCloudId() throws Exception {
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBoltT.execute(getStormTaskTuple());

        assertExecutionResults();


    }

    @Test
    public void shouldSuccessfullyExecuteStormTupleWithCreatingNewCloudIdAndAdditionalLocalIdMapping() throws Exception {
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier",AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(true);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString())).thenReturn(uri);

        oaiWriteRecordBoltT.execute(getStormTaskTupleWithAdditionalLocalIdParam());

        assertExecutionResults();

    }

    @Test
    public void shouldSuccessfullyExecuteStormTupleWhenAdditionalMappingAlreadyExist() throws Exception {
        //given
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);
        CloudException idHasBeenMappedException = new CloudException("", new IdHasBeenMappedException(new ErrorInfo()));
        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier")).thenThrow(idHasBeenMappedException);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(),anyString(),anyString())).thenReturn(uri);

        //when
        oaiWriteRecordBoltT.execute(getStormTaskTupleWithAdditionalLocalIdParam());

        //then
        assertExecutionResults();
    }

    @Test
    public void shouldRetry3BeforeFailingWhenMappingAdditionalLocalId() throws Exception {
        //given
        CloudId cloudId = mock(CloudId.class);
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);

        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier",AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(CloudException.class);
        //when
        oaiWriteRecordBoltT.execute(getStormTaskTupleWithAdditionalLocalIdParam());

        //then
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(4)).createMapping(anyString(), anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void shouldFailWithNoRetriesIfProviderDoestExistWhenMappingAdditionalLocalId() throws Exception {
        //given
        CloudId cloudId = mock(CloudId.class);
        CloudException exception = new CloudException("", new ProviderDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenReturn(cloudId);

        when(uisClient.createMapping(cloudId.getId(), SOURCE + DATA_PROVIDER, "additionalLocalIdentifier",AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(CloudException.class);
        //when
        oaiWriteRecordBoltT.execute(getStormTaskTupleWithAdditionalLocalIdParam());

        //then
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(1)).getCloudId(anyString(), anyString(),anyString(),anyString());
        verify(uisClient, times(0)).createMapping(anyString(), anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void shouldRetry3TimesBeforeFailingWhenCreatingNewCloudId() throws Exception {
        CloudException exception = new CloudException("", new RecordDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        doThrow(CloudException.class).when(uisClient).createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        oaiWriteRecordBoltT.execute(getStormTaskTuple());
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(4)).createCloudId(anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

    @Test
    public void shouldFailWithNoRetriesIfProviderDoestExistWhenCreatingNewCloudId() throws Exception {
        //given
        CloudException exception = new CloudException("", new ProviderDoesNotExistException(new ErrorInfo()));
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER)).thenThrow(exception);
        doThrow(CloudException.class).when(uisClient).createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID,AUTHORIZATION,AUTHORIZATION_HEADER);
        //when
        oaiWriteRecordBoltT.execute(getStormTaskTuple());
        //then
        verify(outputCollector, times(0)).emit(anyList());
        verify(uisClient, times(1)).getCloudId(anyString(), anyString(),anyString(),anyString());
        verify(uisClient, times(0)).createCloudId(anyString(), anyString(),anyString(),anyString());
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION_HEADER);
        parameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE + LOCAL_ID);
        parameters.put(PluginParameterKeys.PROVIDER_ID, SOURCE + DATA_PROVIDER);
        return parameters;
    }


    private void assertExecutionResults() {
        verify(outputCollector, times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(value.size(), 7);
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(parameters.get(PluginParameterKeys.OUTPUT_URL), SOURCE_VERSION_URL);
    }
}

