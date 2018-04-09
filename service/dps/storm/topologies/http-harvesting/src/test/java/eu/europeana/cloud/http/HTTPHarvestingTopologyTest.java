package eu.europeana.cloud.http;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.http.bolt.HTTPHarvesterBolt;
import eu.europeana.cloud.http.helper.HTTPTestMocksHelper;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
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
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Created by Tarek on 3/22/2018.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HTTPHarvesterBolt.class, AddResultToDataSetBolt.class, WriteRecordBolt.class, HarvestingWriteRecordBolt.class, RevisionWriterBolt.class, CassandraConnectionProviderSingleton.class, AddResultToDataSetBolt.class, NotificationBolt.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, ParseTaskBolt.class,MemoryCacheTaskKillerUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*"})
public class HTTPHarvestingTopologyTest extends HTTPTestMocksHelper {

    private final static String FILE_NAME = "http://127.0.0.1:9999/zipFileTest.zip";
    private final static String FILE_NAME2 = "http://127.0.0.1:9999/zipFileTest.tar.gz";
    public static final String SECOND_FILE = "zipFileTest/xml/jedit-4.1.xml";
    public static final String FIRST_FILE = "zipFileTest/jedit-4.0.xml";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    private static final String REPOSITORY_STREAM = "REPOSITORY_URLS";
    private static final String TASK_PARAMETERS = "\"parameters\":" +
            "{\"OUTPUT_DATA_SETS\":\"http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet\"," +
            "\"AUTHORIZATION_HEADER\":\"AUTHORIZATION_HEADER\"}," +
            "\"taskId\":1," +
            "\"outputRevision\":" +
            "{\"revisionName\":\"revisionName\"," +
            "\"revisionProviderId\":\"revisionProvider\"," +
            "\"creationTimeStamp\":1490178872617," +
            "\"published\":false," +
            "\"acceptance\":false," +
            "\"deleted\":false}," +
            "\"taskName\":\"taskName\"}";

    private static Map<String, String> routingRules;
    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_TASK_BOLT, TopologyHelper.HTTP_HARVESTING_BOLT, TopologyHelper.WRITE_RECORD_BOLT, TopologyHelper.REVISION_WRITER_BOLT, TopologyHelper.WRITE_TO_DATA_SET_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);

    public HTTPHarvestingTopologyTest() {

    }

    @BeforeClass
    public static void initTopology() {
        routingRules = new HashMap<>();
        routingRules.put(REPOSITORY_STREAM, REPOSITORY_STREAM);
        buildTopology();

    }

    @Before
    public final void setUp() throws Exception {
        mockRecordSC();
        mockFileSC();
        mockDatSetClient();
        mockRevisionServiceClient();
        mockUISClient();
        configureMocks();
        mockCassandraInteraction();

        wireMockRule.resetAll();
        wireMockRule.stubFor(get(urlEqualTo("/zipFileTest.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipFileTest.zip")));

        wireMockRule.stubFor(get(urlEqualTo("/zipFileTest.tar.gz"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("zipFileTest.tar.gz")));

    }

    private void assertTopology(final String input) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, new Values(input));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"resource\":\"" + FIRST_FILE + "\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]",
                        "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion2/files/FileName2\",\"resource\":\"" + SECOND_FILE + "\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
    }

    @Test
    public final void testHarvestingZipFile() throws MCSException, IOException, URISyntaxException {
        final String input = "{\"inputData\":" +
                "{\"REPOSITORY_URLS\":" +
                "[\"" + FILE_NAME + "\"]}," +
                TASK_PARAMETERS;
        assertTopology(input);

    }

    @Test
    public final void testHarvestingTarGzipFile() throws MCSException, IOException, URISyntaxException {
        final String input = "{\"inputData\":" +
                "{\"REPOSITORY_URLS\":" +
                "[\"" + FILE_NAME2 + "\"]}," +
                TASK_PARAMETERS;
        assertTopology(input);

    }


    private List selectSingle(List actualTuples, int index) {
        return Arrays.asList(actualTuples.get(index));
    }

    private void assertResultedTuple(ILocalCluster cluster, StormTopology topology, CompleteTopologyParam completeTopologyParam, List<String> expectedTuples) throws JSONException {
        Map result = Testing.completeTopology(cluster, topology,
                completeTopologyParam);
        //then
        printDefaultStreamTuples(result);
        List actualTuples = Testing.readTuples(result, TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
        String expected = expectedTuples.get(0);
        for (int i = 0; i < expectedTuples.size(); i++) {
            String actual = parse(selectSingle(actualTuples, i));
            if (actual.contains(SECOND_FILE)) {
                expected = expectedTuples.get(1);
            } else {
                expected = expectedTuples.get(0);
            }
            assertEquals(expected, actual, false);
        }
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private void configureMocks() throws MCSException, IOException, URISyntaxException, CloudException, XMLStreamException, TransformerConfigurationException {
        doNothing().when(uisClient).useAuthorizationHeader(anyString());
        CloudId cloudId = new CloudId();
        cloudId.setId(CLOUD_ID);
        CloudId cloudId2 = new CloudId();
        cloudId2.setId(CLOUD_ID2);
        when(uisClient.getCloudId(anyString(), contains(FIRST_FILE))).thenReturn(cloudId);
        when(uisClient.getCloudId(anyString(), contains(SECOND_FILE))).thenReturn(cloudId2);

        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetClient).useAuthorizationHeader(anyString());
        when(recordServiceClient.createRepresentation(eq(CLOUD_ID), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        when(recordServiceClient.createRepresentation(eq(CLOUD_ID2), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL2));
        when(revisionServiceClient.addRevision(anyString(), anyString(), anyString(), isA(Revision.class))).thenReturn(new URI(REVISION_URL));
        doNothing().when(dataSetClient).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());

    }


    private static void buildTopology() {
        // build the test topology
        WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(MCS_URL, UIS_URL);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(MCS_URL);
        TestInspectionBolt endTest = new TestInspectionBolt();
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules)).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.HTTP_HARVESTING_BOLT, new HTTPHarvesterBolt()).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, REPOSITORY_STREAM);
        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(TopologyHelper.HTTP_HARVESTING_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt).shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.HTTP_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));
        topology = builder.createTopology();
    }
}


