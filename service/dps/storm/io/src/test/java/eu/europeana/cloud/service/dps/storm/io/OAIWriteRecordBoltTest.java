package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Created by Tarek on 7/21/2017.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OAIWriteRecordBolt.class)
public class OAIWriteRecordBoltTest {

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final byte[] FILE_DATA = "Data".getBytes();
    private StormTaskTuple tuple;
    private UISClient uisClient;
    private RecordServiceClient recordServiceClient;

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;


    @InjectMocks
    private OAIWriteRecordBolt oaiWriteRecordBoltT = new OAIWriteRecordBolt("http://localhost:8080/mcs", "http://localhost:8080/uis");

    @Before
    public void init() throws Exception {
        when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
        MockitoAnnotations.initMocks(this);
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<String>(Arrays.asList(SOURCE + REPRESENTATION_NAME)));
        tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), new Revision(), oaipmhHarvestingDetails);
        mockUisClient();
        mockRecordServiceClient();
    }


    @Test
    public void successfulExecuteStormTupleWithExistedCloudId() throws Exception {
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(uri);

        oaiWriteRecordBoltT.execute(tuple);

        assertExecutionResults();

    }

    @Test
    public void successfulExecuteStormTupleWithCreatingNewCloudId() throws Exception {
        CloudId cloudId = mock(CloudId.class);
        when(cloudId.getId()).thenReturn(SOURCE + CLOUD_ID);
        when(uisClient.getCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(null);
        when(uisClient.createCloudId(SOURCE + DATA_PROVIDER, SOURCE + LOCAL_ID)).thenReturn(cloudId);
        URI uri = new URI(SOURCE_VERSION_URL);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(uri);

        oaiWriteRecordBoltT.execute(tuple);

        assertExecutionResults();

    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.OAI_IDENTIFIER, SOURCE + LOCAL_ID);
        parameters.put(PluginParameterKeys.PROVIDER_ID, SOURCE + DATA_PROVIDER);
        return parameters;
    }

    private void mockRecordServiceClient() throws Exception {
        recordServiceClient = mock(RecordServiceClient.class);
        whenNew(RecordServiceClient.class).withArguments(anyString()).thenReturn(recordServiceClient);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
    }

    private void mockUisClient() throws Exception {
        uisClient = mock(UISClient.class);
        whenNew(UISClient.class).withArguments(anyString()).thenReturn(uisClient);
        doNothing().when(uisClient).useAuthorizationHeader(anyString());
    }

    private void assertExecutionResults() {
        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values value = captor.getAllValues().get(0);
        assertEquals(value.size(), 7);
        assertTrue(value.get(4) instanceof Map);
        Map<String, String> parameters = (Map<String, String>) value.get(4);
        assertNotNull(parameters.get(PluginParameterKeys.OUTPUT_URL));
        assertEquals(parameters.get(PluginParameterKeys.OUTPUT_URL), SOURCE_VERSION_URL);
    }


}

