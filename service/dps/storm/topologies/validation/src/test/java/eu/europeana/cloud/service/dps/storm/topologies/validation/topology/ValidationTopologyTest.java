package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.StatisticsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper.ValidationMockHelper;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.json.JSONException;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Created by Tarek on 12/5/2017.
 */

//TODO Replaced with mockito runner, cause test had exited Vm machine and broke other tests. Anyway test had not passed.
@RunWith(MockitoJUnitRunner.class)
//@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, ValidationBolt.class, ValidationRevisionWriter.class, NotificationBolt.class, StatisticsBolt.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, CassandraNodeStatisticsDAO.class, TaskStatusChecker.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*", "eu.europeana.cloud.test.CassandraTestInstance"})
@Ignore //There are only ignored tests in this class - so the class must have this annotation
// in other case tests finish with error: No tests found in ValidationTopologyTest
public class ValidationTopologyTest extends ValidationMockHelper {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));
    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.VALIDATION_BOLT, TopologyHelper.STATISTICS_BOLT, TopologyHelper.REVISION_WRITER_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);


    @BeforeClass
    public static void buildToplogy() {
        buildTopology();
    }

    @Before
    public final void setUp() throws Exception {
        mockRecordSC();
        mockFileSC();
        mockCassandraInteraction();
        mockDatSetClient();
        mockRevisionServiceClient();
        mockRepresentationIterator();
        configureMocks();

        wireMockRule.resetAll();
        wireMockRule.stubFor(get(urlEqualTo("/test_schema.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("test_schema.zip")));
        wireMockRule.stubFor(get(urlEqualTo("/edm_sorter.xsl"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("edm_sorter.xsl")));

    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"The record is validated correctly\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
    }


    @Ignore
    public final void testBasicTopology() throws MCSException, IOException, URISyntaxException {
        //given
        prepareForFileUrls();

        DpsTask dpsTask = new DpsTask();
        Map<String, String> taskParameters = new HashMap<>();
        taskParameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        taskParameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.AUTHORIZATION_HEADER);
        taskParameters.put(PluginParameterKeys.SCHEMA_NAME, "edm-internal");
        taskParameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE_VERSION_URL);


        dpsTask.setParameters(taskParameters);
        dpsTask.setInputData(null);
        dpsTask.setOutputRevision(new Revision());
        dpsTask.setHarvestingDetails(new OAIPMHHarvestingDetails());
        dpsTask.setTaskName("Task_Name");

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                dpsTask.getTaskId(),
                dpsTask.getTaskName(),
                null, null, taskParameters, dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());

        assertTopology(stormTaskTuple);

    }

    @Ignore
    public final void testBasicTopologyExternal() throws MCSException, IOException, URISyntaxException {
        //given
        prepareForFileForExternalUrls();

        DpsTask dpsTask = new DpsTask();
        Map<String, String> taskParameters = new HashMap<>();
        taskParameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        taskParameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.AUTHORIZATION_HEADER);
        taskParameters.put(PluginParameterKeys.SCHEMA_NAME, "edm-external");
        taskParameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE_VERSION_URL);


        dpsTask.setParameters(taskParameters);
        dpsTask.setInputData(null);
        dpsTask.setOutputRevision(new Revision());
        dpsTask.setHarvestingDetails(new OAIPMHHarvestingDetails());
        dpsTask.setTaskName("Task_Name");

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                dpsTask.getTaskId(),
                dpsTask.getTaskName(),
                null, null, taskParameters, dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());

        assertTopology(stormTaskTuple);

    }


    private final void prepareForFileUrls() throws URISyntaxException, IOException, MCSException {
        prepareMockFileUrls();
        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"))));
    }


    private final void prepareForFileForExternalUrls() throws URISyntaxException, IOException, MCSException {
        prepareMockFileUrls();
        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/Item_35834473.xml"))));
    }

    private final void prepareMockFileUrls() throws URISyntaxException {
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
    }


    private void configureMocks() throws MCSException, IOException, URISyntaxException {
        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetClient).useAuthorizationHeader(anyString());
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
    }


    private void assertResultedTuple(ILocalCluster cluster, StormTopology topology, CompleteTopologyParam completeTopologyParam, List<String> expectedTuples) throws JSONException {
        Map result = Testing.completeTopology(cluster, topology,
                completeTopologyParam);
        //then
        printDefaultStreamTuples(result);
        List actualTuples = Testing.readTuples(result, TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
        for (int i = 0; i < expectedTuples.size(); i++) {
            String actual = parse(selectSingle(actualTuples, i));
            String expected = expectedTuples.get(i);
            assertEquals(expected, actual, false);
        }
    }

    private List selectSingle(List actualTuples, int index) {
        return Arrays.asList(actualTuples.get(index));
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private static void buildTopology() {
        // build the test topology
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        StatisticsBolt statisticsBolt = new StatisticsBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.VALIDATION_BOLT, new ValidationBolt(readProperties("validation.properties"))).shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.STATISTICS_BOLT, statisticsBolt).shuffleGrouping(TopologyHelper.VALIDATION_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, new ValidationRevisionWriter(MCS_URL, ValidationTopology.SUCCESS_MESSAGE)).shuffleGrouping(TopologyHelper.STATISTICS_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.VALIDATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.STATISTICS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));

        topology = builder.createTopology();
    }

    private static Properties readProperties(String propertyFilename) {
        Properties props = new Properties();
        PropertyFileLoader.loadPropertyFile(propertyFilename, "", props);
        return props;
    }
}