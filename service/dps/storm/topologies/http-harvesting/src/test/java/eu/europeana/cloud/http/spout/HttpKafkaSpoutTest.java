package eu.europeana.cloud.http.spout;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.apache.storm.spout.SpoutOutputCollector;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 5/4/2018.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpKafkaSpout.class)
@PowerMockIgnore("javax.net.ssl.*")
public class HttpKafkaSpoutTest {
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock(name = "cache")
    private ConcurrentHashMap<Long, TaskSpoutInfo> cache;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final static int FILES_COUNT = 13;

    private final static String FILE_NAME = "http://127.0.0.1:9999/ZipFilesWithMixedCompressedFiles.zip";
    private final static String FILE_NAME2 = "http://127.0.0.1:9999/gzFilesWithMixedCompressedFiles.tar.gz";
    private final static String FILE_NAME3 = "http://127.0.0.1:9999/gzFileWithCompressedGZFiles.tgz";
    private final static String FILE_NAME4 = "http://127.0.0.1:9999/zipFileFromMac.zip";


    @InjectMocks
    private HttpKafkaSpout httpKafkaSpout = new HttpKafkaSpout(null);

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

        wireMockRule.stubFor(get(urlEqualTo("/gzFileWithCompressedGZFiles.tgz"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("gzFileWithCompressedGZFiles.tgz")));

        wireMockRule.stubFor(get(urlEqualTo("/zipFileFromMac.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipFileFromMac.zip")));
        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        TaskSpoutInfo taskSpoutInfo = mock(TaskSpoutInfo.class);
        when(cache.get(anyLong())).thenReturn(taskSpoutInfo);
        doNothing().when(taskSpoutInfo).inc();
    }

    @Test
    public void shouldHarvestTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldHarvestTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME2, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldHarvesshouldTGZFileRecursivelyWithCompressedXMLFiles() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME3, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldHarvesshouldZipFileFromMacIgnoringExtraCopies() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME4, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void TheHarvestingShouldFailForNonExistedURL() throws Exception {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, "UNDEFINED_URL", null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertFailedHarvesting();
    }


    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        return parameters;
    }

    private void assertSuccessfulHarvesting() {
        Mockito.verify(collector, Mockito.times(FILES_COUNT)).emit(any(List.class));

    }

    private void assertFailedHarvesting() {
        Mockito.verify(collector, Mockito.times(0)).emit(any(List.class));

    }


}