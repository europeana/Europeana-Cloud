package eu.europeana.cloud.http.spout.job;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.http.spout.HttpKafkaSpout;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class TaskExecutorTest {
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final static int FILES_COUNT = 13;
    private static final int MAX_SIZE = 100;

    private final static String FILE_NAME = "http://127.0.0.1:9999/ZipFilesWithMixedCompressedFiles.zip";
    private final static String FILE_NAME2 = "http://127.0.0.1:9999/gzFilesWithMixedCompressedFiles.tar.gz";
    private final static String FILE_NAME3 = "http://127.0.0.1:9999/gzFileWithCompressedGZFiles.tgz";
    private final static String FILE_NAME4 = "http://127.0.0.1:9999/zipFileFromMac.zip";
    private final static String FILE_NAME5 = "http://127.0.0.1:9999/zipWithEDMs.zip";
    private final static String FILE_NAME6 = "http://127.0.0.1:9999/zipWithCorruptedEDM.zip";
    private final static String FILE_NAME7 = "http://127.0.0.1:9999/zipWithCorrectAndCorruptedEDM.zip";

    private ArrayBlockingQueue<StormTaskTuple> tuplesWithFileUrls = new ArrayBlockingQueue(MAX_SIZE);

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
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
        tuplesWithFileUrls.clear();
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

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 1);
        StormTaskTuple stormTaskTuple = tuplesWithFileUrls.poll();
        assertEquals("/123123/object_DCU_24927017", stormTaskTuple.getParameter("CLOUD_LOCAL_IDENTIFIER"));
        assertEquals("http://more.locloud.eu/object/DCU/24927017", stormTaskTuple.getParameter("ADDITIONAL_LOCAL_IDENTIFIER"));
    }

    @Test
    public void shouldHarvestTheZipFileAndUseOriginalIdentifier() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME5, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 1);
        StormTaskTuple stormTaskTuple = tuplesWithFileUrls.poll();
        assertTrue(stormTaskTuple.getParameter("CLOUD_LOCAL_IDENTIFIER").toString().startsWith("record.xml"));
    }

    @Test
    public void shouldEmitErrorNotificationWhenUsingEuropeanaIdsAndMetisDatasetIdNotProvided() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertFailedHarvesting();
    }

    @Test
    public void shouldEmitErrorNotificationWhenUsingEuropeanaIdsAndMetisDatasetIdIsEmpty() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertFailedHarvesting();
    }


    @Test
    public void shouldEmitErrorNotificationWhenCannotCreateEuropeanaId() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        HashMap<String, String> parameters = prepareStormTaskTupleParameters();
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "false");
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, "123123");
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME6, null, parameters, new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

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

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertEquals(tuplesWithFileUrls.size(), 1);
    }

    @Test
    public void shouldHarvestTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTheZipFilesWithNestedMixedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false, false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }

    @Test
    public void shouldHarvestTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME2, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTarGzFilesRecursivelyWithMixedNestedCompressedFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false, false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME2, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }

    @Test
    public void shouldHarvestTGZFileRecursivelyWithCompressedXMLFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME3, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertSuccessfulHarvesting();
    }

    @Test
    public void shouldCancelTheTaskForTForTGZFileRecursivelyWithCompressedXMLFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false, false, true);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME3, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        Mockito.verify(collector, Mockito.atMost(2)).emit(any(List.class));
    }


    @Test
    public void shouldHarvestZipFileFromMacIgnoringExtraCopies() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, FILE_NAME4, null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  any(DpsTask.class));
        taskExecutor.call();

        assertSuccessfulHarvesting();
    }

    @Test(expected = IOException.class)
    public void harvestingShouldFailForNonExistedURL() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, "UNDEFINED_URL", null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  new DpsTask());
        taskExecutor.execute();
    }


    @Test
    public void shouldDropTaskInCaseOfException() throws Exception {
        when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, "UNDEFINED_URL", null, prepareStormTaskTupleParameters(), new Revision());

        TaskExecutor taskExecutor = new TaskExecutor(collector, taskStatusChecker, cassandraTaskInfoDAO,
                tuplesWithFileUrls, tuple,  new DpsTask());
        taskExecutor.call();

        //called twice, once per finally inside TaskExecutor.execute() and the other inside call() catch exception block
        verify(cassandraTaskInfoDAO, times(2)).dropTask(anyLong(), anyString(), anyString());
    }


    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, "true");
        return parameters;
    }

    private void assertSuccessfulHarvesting() {
        assertEquals(tuplesWithFileUrls.size(), FILES_COUNT);

    }

    private void assertFailedHarvesting() {
        assertTrue(tuplesWithFileUrls.isEmpty());
    }
}
