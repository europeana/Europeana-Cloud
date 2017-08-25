package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import com.lyncode.xml.exceptions.XmlWriteException;
import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.exceptions.CannotDisseminateFormatException;
import com.lyncode.xoai.serviceprovider.exceptions.IdDoesNotExistException;
import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.TaskSplittingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.OAITestMocksHelper;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

/**
 * Created by Tarek on 7/26/2017.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskSplittingBolt.class, IdentifiersHarvestingBolt.class, RecordHarvestingBolt.class, AddResultToDataSetBolt.class, WriteRecordBolt.class, OAIWriteRecordBolt.class, RevisionWriterBolt.class, CassandraConnectionProviderSingleton.class, AddResultToDataSetBolt.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, ParseTaskBolt.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class OAIPHMHarvestingTopologyTest extends OAITestMocksHelper {

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
            "\"harvestingDetails\":" +
            "{\"schemas\":[\"Schema\"]," +
            "\"dateFrom\":\"2012-03-15\"," +
            "\"dateUntil\":\"2012-04-10\"}," +
            "\"taskName\":\"taskName\"}";

    private static Map<String, String> routingRules;
    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_TASK_BOLT, TopologyHelper.TASK_SPLITTING_BOLT, TopologyHelper.IDENTIFIERS_HARVESTING_BOLT, TopologyHelper.RECORD_HARVESTING_BOLT, TopologyHelper.WRITE_RECORD_BOLT, TopologyHelper.REVISION_WRITER_BOLT, TopologyHelper.WRITE_TO_DATA_SET_BOLT, TEST_END_BOLT);

    public OAIPHMHarvestingTopologyTest() {

    }

    @BeforeClass
    public static void initTopology() {
        routingRules = new HashMap<>();
        routingRules.put(REPOSITORY_STREAM, REPOSITORY_STREAM);
        buildTopology();

    }

    @Before
    public final void setUp() throws Exception {
        mockZookeeperKS();
        mockRecordSC();
        mockFileSC();
        mockSourceProvider();
        mockOAIClientProvider();
        mockDatSetClient();
        mockRevisionServiceClient();
        mockUISClient();
        configureMocks();
    }

    private void assertTopology(final String input) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, new Values(input));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]",
                        "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
    }

    @Test
    public final void testBasicTopology() throws MCSException, IOException, URISyntaxException {
        //given
        prepareForSingleDataset();

        final String input = "{\"inputData\":" +
                "{\"REPOSITORY_URLS\":" +
                "[\"" + SOURCE_VERSION_URL + "\"]}," +
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
        for (int i = 0; i < expectedTuples.size(); i++) {
            String actual = parse(selectSingle(actualTuples, i));
            String expected = expectedTuples.get(i);
            assertEquals(expected,actual,false);
        }
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private void configureMocks() throws MCSException, IOException, URISyntaxException, BadArgumentException, OAIRequestException, CloudException, HarvesterException, CannotDisseminateFormatException, XmlWriteException, IdDoesNotExistException, XMLStreamException, TransformerConfigurationException {
        ServiceProvider serviceProvider = mock(ServiceProvider.class);
        when(sourceProvider.provide(anyString())).thenReturn(serviceProvider);
        Set<Header> headers = new HashSet<>();
        headers.add(new Header().withIdentifier("ID1").withSetSpec(null).withDatestamp(new Date()));
        headers.add(new Header().withIdentifier("ID2").withSetSpec(null).withDatestamp(new Date()));
        when(serviceProvider.listIdentifiers(any(ListIdentifiersParameters.class))).thenReturn(headers.iterator());

        OAIClient oaiClient = mock(OAIClient.class);
        when(harvester.harvestRecord(anyString(),anyString(),anyString())).thenReturn(new
                ByteArrayInputStream(new byte[]{}));
        InputStream inputStream = new ReaderInputStream(new StringReader("largeString"), StandardCharsets.UTF_8);
        when(oaiClient.execute(any(Parameters.class))).thenReturn(inputStream);


        doNothing().when(uisClient).useAuthorizationHeader(anyString());
        CloudId cloudId = new CloudId();
        cloudId.setId(CLOUD_ID);
        when(uisClient.getCloudId(anyString(), anyString())).thenReturn(cloudId);

        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetClient).useAuthorizationHeader(anyString());
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
    }

    private final void prepareForSingleDataset() throws URISyntaxException, IOException, MCSException {

        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream("testContent".getBytes()));

        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        when(revisionServiceClient.addRevision(anyString(), anyString(), anyString(), isA(Revision.class))).thenReturn(new URI(REVISION_URL));
        doNothing().when(dataSetClient).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString());

    }

    private static void buildTopology() {
        // build the test topology
        WriteRecordBolt writeRecordBolt = new OAIWriteRecordBolt(MCS_URL, UIS_URL);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(MCS_URL);
        TestInspectionBolt endTest = new TestInspectionBolt();
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(MCS_URL);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules)).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.TASK_SPLITTING_BOLT, new TaskSplittingBolt(2592000000l)).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, REPOSITORY_STREAM);
        builder.setBolt(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT, new IdentifiersHarvestingBolt()).shuffleGrouping(TopologyHelper.TASK_SPLITTING_BOLT);
        builder.setBolt(TopologyHelper.RECORD_HARVESTING_BOLT, new RecordHarvestingBolt()).shuffleGrouping(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT);
        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(TopologyHelper.RECORD_HARVESTING_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt).shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);


        topology = builder.createTopology();


    }

    private CompleteTopologyParam prepareCompleteTopologyParam(MockedSources mockedSources) {
        Config conf = new Config();
        conf.setNumWorkers(NUM_WORKERS);
        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedSources);
        completeTopologyParam.setStormConf(conf);
        return completeTopologyParam;
    }
}
