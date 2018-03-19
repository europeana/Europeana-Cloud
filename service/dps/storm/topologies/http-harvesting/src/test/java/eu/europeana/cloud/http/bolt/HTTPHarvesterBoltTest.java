package eu.europeana.cloud.http.bolt;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;


@RunWith(PowerMockRunner.class)
@PrepareForTest(HTTPHarvesterBolt.class)
@PowerMockIgnore("javax.net.ssl.*")
public class HTTPHarvesterBoltTest {
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final static int FILES_COUNT = 13;

    private final static String FILE_NAME = "http://127.0.0.1:9999/ZipFilesWithMixedCompressedFiles.zip";
    private final static String FILE_NAME2 = "http://127.0.0.1:9999/gzFilesWithMixedCompressedFiles.tar.gz";


    @InjectMocks
    private HTTPHarvesterBolt httpHarvesterBolt = new HTTPHarvesterBolt();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        wireMockRule.resetAll();
        wireMockRule.stubFor(get(urlEqualTo("/ZipFilesWithMixedCompressedFiles.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("ZipFilesWithMixedCompressedFiles.zip")));

        wireMockRule.stubFor(get(urlEqualTo("/gzFilesWithMixedCompressedFiles.tar.gz"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("gzFilesWithMixedCompressedFiles.tar.gz")));
    }

    @Test
    public void shouldHarvestTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, null, prepareStormTaskTupleParameters(FILE_NAME), new Revision());
        httpHarvesterBolt.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldHarvestTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, null, prepareStormTaskTupleParameters(FILE_NAME2), new Revision());
        httpHarvesterBolt.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void TheHarvestingShouldFailForNonExistedURL() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, null, prepareStormTaskTupleParameters("UNDEFINED_URL"), new Revision());
        httpHarvesterBolt.execute(tuple);
        assertFailedHarvesting();
    }


    private HashMap<String, String> prepareStormTaskTupleParameters(String compressedFileURL) throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, compressedFileURL);
        return parameters;
    }

    private void assertSuccessfulHarvesting() {
        Mockito.verify(outputCollector, Mockito.times(FILES_COUNT)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private void assertFailedHarvesting() {
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }


}