package eu.europeana.cloud.http.bolts;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.hamcrest.core.StringStartsWith;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class HttpHarvestingBoltTest {

    private static final String FILE_URL = "http://test-app1/http_harvest/task-2709280817814521453/extracted/Lithuania_280/Lithuania_1.xml";
    private final String TASK_NAME = "TASK_NAME";
    private final int TASK_ID = 1;
    private StormTaskTuple tuple ;

    @InjectMocks
    private HttpHarvestingBolt bolt;

    @Mock
    private Tuple anchorTuple;

    @Mock
    private OutputCollector outputCollector;

    @Captor
    private ArgumentCaptor<List<Object>> resultTupleCaptor;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    @Before
    public void setup() throws IllegalAccessException {
        wireMockRule.resetAll();
        tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_URL, null, prepareStormTaskTupleParameters(), new Revision());
        PowerMockito.field(HttpHarvestingBolt.class,"SLEEP_TIME_BETWEEN_RETRIES_MS").setInt(bolt,100);
        bolt.prepare();
    }

    @Test
    public void shouldHarvestEdmFileWhenExecutedWithoutUseDefaultIdentifiersParam() throws IOException {
        tuple.setFileUrl("http://localhost:9999/record.xml");
        mockFileOnHttpServer("record.xml");

        bolt.execute(anchorTuple,tuple);

        verify(outputCollector).emit(eq(anchorTuple),resultTupleCaptor.capture());
        StormTaskTuple resultTuple = getResultStormTaskTuple();
        assertArrayEquals(readTestFile("record.xml"),resultTuple.getFileData());
        assertEquals("/100/object_DCU_24927017",resultTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
        assertEquals("http://more.locloud.eu/object/DCU/24927017",resultTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals(MediaType.APPLICATION_XML, resultTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE));
    }

    @Test
    public void shouldHarvestEdmFileWhenExecutedWithUseDefaultIdentifiersParamSetToTrue() throws IOException {
        tuple.addParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "true");
        tuple.setFileUrl("http://localhost:9999/record.xml");
        tuple.addParameter(PluginParameterKeys.FILES_ROOT_URL,"http://localhost:9999/");
        mockFileOnHttpServer("record.xml");

        bolt.execute(anchorTuple,tuple);

        verify(outputCollector).emit(eq(anchorTuple), resultTupleCaptor.capture());
        StormTaskTuple resultTuple = getResultStormTaskTuple();
        assertArrayEquals(readTestFile("record.xml"),resultTuple.getFileData());
        assertThat(resultTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER), StringStartsWith.startsWith("record.xml"));
        assertEquals(MediaType.APPLICATION_XML, resultTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE));

    }


    @Test
    public void shouldRetryWhenCantDownloadFileFirstTime() throws IOException, IllegalAccessException {
        tuple.setFileUrl("http://localhost:9999/record.xml");
        mockErrorOnHttpOnFirstTryServer("record.xml");

        bolt.execute(anchorTuple,tuple);

        verify(outputCollector).emit(eq(anchorTuple), resultTupleCaptor.capture());
        StormTaskTuple resultTuple = getResultStormTaskTuple();
        assertArrayEquals(readTestFile("record.xml"),resultTuple.getFileData());
        assertEquals("/100/object_DCU_24927017",resultTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
        assertEquals("http://more.locloud.eu/object/DCU/24927017",resultTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals(MediaType.APPLICATION_XML, resultTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE));
    }

    @Test
    public void shouldEmitErrorNotificationWhenCantLoadFilePermanently() throws IOException {
        tuple.setFileUrl("http://localhost:9999/record.xml");
        mockErrorOnHttpOnServer("record.xml");

        bolt.execute(anchorTuple,tuple);

        verify(outputCollector,never()).emit(eq(anchorTuple), any());
        verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME),eq(anchorTuple),resultTupleCaptor.capture());
    }

    private byte[] readTestFile(String fileName) throws IOException {
        return getClass().getResourceAsStream("/__files/"+fileName).readAllBytes();
    }

    private void mockFileOnHttpServer(String fileName) {
        wireMockRule.stubFor(get(urlEqualTo("/" +fileName))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(150)
                        .withBodyFile(fileName)));
    }

    private void mockErrorOnHttpOnServer(String fileName) {
        wireMockRule.stubFor(get(urlEqualTo("/" +fileName))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withFixedDelay(150)));

    }

    private void mockErrorOnHttpOnFirstTryServer(String fileName) {
        wireMockRule.stubFor(get(urlEqualTo("/" +fileName))
                .inScenario("Retry")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse()
                        .withStatus(500)
                        .withFixedDelay(150))
                        .willSetStateTo("retried"));

        wireMockRule.stubFor(get(urlEqualTo("/" +fileName))
                .inScenario("Retry")
                .whenScenarioStateIs("retried")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(150)
                        .withBodyFile(fileName)));
    }

    private StormTaskTuple getResultStormTaskTuple() {
        return StormTaskTuple.fromValues(resultTupleCaptor.getValue());
    }

    private HashMap<String, String> prepareStormTaskTupleParameters()  {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "100");
        return parameters;
    }
}