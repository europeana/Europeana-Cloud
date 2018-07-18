package eu.europeana.cloud.http.spout;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.*;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

/**
 * Created by Tarek on 5/4/2018.
 */

public class HttpKafkaSpoutTest {
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;


    @Mock
    private TaskStatusChecker taskStatusChecker;

    @Mock(name = "cache")
    private ConcurrentHashMap<Long, TaskSpoutInfo> cache;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final static int FILES_COUNT = 13;

    private final static String FILE_NAME = "http://127.0.0.1:9999/ZipFilesWithMixedCompressedFiles.zip";
    private final static String FILE_NAME2 = "http://127.0.0.1:9999/gzFilesWithMixedCompressedFiles.tar.gz";
    private final static String FILE_NAME3 = "http://127.0.0.1:9999/gzFileWithCompressedGZFiles.tgz";
    private final static String FILE_NAME4 = "http://127.0.0.1:9999/zipFileFromMac.zip";
    private final static String FILE_NAME5 = "http://127.0.0.1:9999/zipWithEDMs.zip";
    private final static String FILE_NAME6 = "http://127.0.0.1:9999/zipWithCorruptedEDM.zip";
    private final static String FILE_NAME7 = "http://127.0.0.1:9999/zipWithCorrectAndCorruptedEDM.zip";

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @InjectMocks
    private HttpKafkaSpout httpKafkaSpout = new HttpKafkaSpout(null);

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    @Before
    public void init() throws Exception {
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
        wireMockRule.stubFor(get(urlEqualTo("/zipWithEDMs.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipWithEDMs.zip")));
        wireMockRule.stubFor(get(urlEqualTo("/zipWithCorruptedEDM.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipWithCorruptedEDM.zip")));
        wireMockRule.stubFor(get(urlEqualTo("/zipWithCorrectAndCorruptedEDM.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipWithCorrectAndCorruptedEDM.zip")));

        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        TaskSpoutInfo taskSpoutInfo = mock(TaskSpoutInfo.class);
        when(cache.get(anyLong())).thenReturn(taskSpoutInfo);
        doNothing().when(taskSpoutInfo).inc();
        setStaticField(HttpKafkaSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
    }

    static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void shouldHarvestTheZipFileAndExtractIdentifiersFromFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "123123");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME5, null, parameters, new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(4);
        assertEquals("/123123/object_DCU_24927017", val.get("CLOUD_LOCAL_IDENTIFIER"));
        assertEquals("http://more.locloud.eu/object/DCU/24927017", val.get("ADDITIONAL_LOCAL_IDENTIFIER"));
    }

    @Test
    public void shouldHarvestTheZipFileAndUseOriginalIdentifier() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME5, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(4);
        assertTrue(val.get("CLOUD_LOCAL_IDENTIFIER").toString().startsWith("record.xml"));
    }

    @Test
    public void shouldEmitErrorNotificationWhenUsingEuropeanaIdsAndMetisDatasetIdNotProvided() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());
        httpKafkaSpout.execute(tuple);
        assertFailedHarvesting();
    }

    @Test
    public void shouldEmitErrorNotificationWhenUsingEuropeanaIdsAndMetisDatasetIdIsEmpty() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());
        httpKafkaSpout.execute(tuple);
        assertFailedHarvesting();
    }


    @Test
    public void shouldEmitErrorNotificationWhenCannotCreateEuropeanaId() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "123123");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class));
    }


    @Test
    public void shouldEmitErrorNotificationForBrokenRecordsWhenCannotCreateEuropeanaId() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "123123");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME7, null, parameters, new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.any(List.class)); //1 record harvested correctly
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class)); //1 record failed
    }

    @Test
    public void shouldHarvestTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false,false,true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }

    @Test
    public void shouldHarvestTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME2, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false,false,true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME2, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }

    @Test
    public void shouldHarvestTGZFileRecursivelyWithCompressedXMLFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME3, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTForTGZFileRecursivelyWithCompressedXMLFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false,false,true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME3, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }


    @Test
    public void shouldHarvestZipFileFromMacIgnoringExtraCopies() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME4, null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertSuccessfulHarvesting();
    }

    @Test
    public void TheHarvestingShouldFailForNonExistedURL() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, "UNDEFINED_URL", null, prepareStormTaskTupleParameters(), new Revision());
        httpKafkaSpout.execute(tuple);
        assertFailedHarvesting();
    }


    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "true");
        return parameters;
    }

    private void assertSuccessfulHarvesting() {
        Mockito.verify(collector, Mockito.times(FILES_COUNT)).emit(any(List.class));

    }

    private void assertFailedHarvesting() {
        Mockito.verify(collector, Mockito.times(0)).emit(any(List.class));
    }

}