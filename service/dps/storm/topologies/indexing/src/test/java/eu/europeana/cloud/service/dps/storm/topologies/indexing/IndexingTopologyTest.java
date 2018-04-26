package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import com.mongodb.util.JSONParseException;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, ReadDatasetsBolt.class, ReadRepresentationBolt.class, ReadDatasetBolt.class, IndexingBolt.class, NotificationBolt.class, ValidationRevisionWriter.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, TaskStatusChecker.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*"})
public class IndexingTopologyTest extends TopologyTestHelper {

    protected RepresentationIterator representationIterator;

    private static Map<String, String> routingRules;
    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_TASK_BOLT, TopologyHelper.READ_DATASETS_BOLT, TopologyHelper.READ_DATASET_BOLT, TopologyHelper.READ_REPRESENTATION_BOLT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.INDEXING_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);
    private static final String DATASET_STREAM = "DATASET_URLS";
    private static final String FILE_STREAM = "FILE_URLS";
    private static final String TASK_PARAMETERS_WITH_OUTPUT_REVISION = "\"parameters\":" +
            "{\"REPRESENTATION_NAME\":\"" + SOURCE + REPRESENTATION_NAME + "\"," +
            "\"AUTHORIZATION_HEADER\":\"AUTHORIZATION_HEADER\"," +
            "\"TARGET_INDEXING_DATABASE\":\"PREVIEW\"}," +
            "\"taskId\":1," +
            "\"outputRevision\":" +
            "{\"revisionName\":\"revisionName\"," +
            "\"revisionProviderId\":\"revisionProvider\"," +
            "\"creationTimeStamp\":1490178872617," +
            "\"published\":false," +
            "\"acceptance\":false," +
            "\"deleted\":false}," +
            "\"taskName\":\"taskName\"}";

    @BeforeClass
    public static void prepareTopology() {
        routingRules = new HashMap<>();
        routingRules.put(DATASET_STREAM, DATASET_STREAM);
        routingRules.put(FILE_STREAM, FILE_STREAM);
        buildTopology();
    }

    @Before
    public final void setUp() throws Exception {
        mockRecordSC();
        mockFileSC();
        mockDatSetClient();
        mockRevisionServiceClient();
        mockCassandraInteraction();
    }

    private static void buildTopology() {
        // build the test topology
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(MCS_URL);
        ReadDatasetsBolt readDatasetsBolt = new ReadDatasetsBolt();
        ReadDatasetBolt readDataSetBolt = new ReadDatasetBolt(MCS_URL);
        ReadRepresentationBolt readRepresentationBolt = new ReadRepresentationBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules)).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.READ_DATASETS_BOLT, readDatasetsBolt).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, DATASET_STREAM);
        builder.setBolt(TopologyHelper.READ_DATASET_BOLT, readDataSetBolt).shuffleGrouping(TopologyHelper.READ_DATASETS_BOLT);
        builder.setBolt(TopologyHelper.READ_REPRESENTATION_BOLT, readRepresentationBolt).shuffleGrouping(TopologyHelper.READ_DATASET_BOLT);
        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, FILE_STREAM)
                .shuffleGrouping(TopologyHelper.READ_REPRESENTATION_BOLT);
        builder.setBolt(TopologyHelper.INDEXING_BOLT, new IndexingBolt(readProperties("indexing.properties"))).shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, new ValidationRevisionWriter(MCS_URL, IndexingTopology.SUCCESS_MESSAGE)).shuffleGrouping(TopologyHelper.INDEXING_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASETS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_REPRESENTATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));

        topology = builder.createTopology();
    }

    private static Properties readProperties(String propertyFilename) {
        Properties props = new Properties();
        PropertyFileLoader.loadPropertyFile(propertyFilename, "", props);
        return props;
    }

    @Test
    public final void shouldProcessMultipleFiles() throws Exception {
        //mocking
        IndexerFactory indexerFactory = Mockito.mock(IndexerFactory.class);
        Indexer indexer = Mockito.mock(Indexer.class);
        PowerMockito.whenNew(IndexerFactory.class).withAnyArguments().thenReturn(indexerFactory);
        Mockito.when(indexerFactory.getIndexer()).thenReturn(indexer);
        //
        prepareMockFileUrls();
        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream(new byte[]{'a','b','c'}));
        when(fileServiceClient.getFile(SOURCE_VERSION_URL_FILE2)).thenReturn(new ByteArrayInputStream(new byte[]{'b','c','d'}));
        //
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);
        //

        //given
        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"" + SOURCE_VERSION_URL + "," + SOURCE_VERSION_URL_FILE2 + "\"]}," +
                TASK_PARAMETERS_WITH_OUTPUT_REVISION;
        assertTopology(input);

    }

    @Test
    public final void shouldProcessDatasetFiles() throws Exception {
        prepareForDataset();

        //mocking
        IndexerFactory indexerFactory = Mockito.mock(IndexerFactory.class);
        Indexer indexer = Mockito.mock(Indexer.class);
        PowerMockito.whenNew(IndexerFactory.class).withAnyArguments().thenReturn(indexerFactory);
        Mockito.when(indexerFactory.getIndexer()).thenReturn(indexer);
        //
        prepareMockFileUrls();
        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream(new byte[]{'a','b','c'}));
        when(fileServiceClient.getFile(SOURCE_VERSION_URL_FILE2)).thenReturn(new ByteArrayInputStream(new byte[]{'b','c','d'}));
        //
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);
        //

        //given
        final String input = "{\"inputData\":" +
                "{\"DATASET_URLS\":" +
                "[\"" + SOURCE_DATASET_URL + "\"]}," +
                TASK_PARAMETERS_WITH_OUTPUT_REVISION;

        assertTopology(input);
    }

    private void prepareForDataset() throws URISyntaxException, IOException, MCSException {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        List<File> files = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        files.add(new File("sourceFileName", "application/xml", "md5", "1", 5, new URI(SOURCE_VERSION_URL)));
        files.add(new File("sourceFileName2", "application/xml", "md5", "1", 5, new URI(SOURCE_VERSION_URL_FILE2)));

        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, files, revisions, false, new Date());

        when(dataSetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(representation);
        when(revisionServiceClient.addRevision(anyString(), anyString(), anyString(), isA(Revision.class))).thenReturn(new URI(REVISION_URL));
    }

    protected void mockRevisionServiceClient() throws Exception {
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);
    }

    private void prepareMockFileUrls() throws URISyntaxException {
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE2)).thenReturn(new URI(SOURCE_VERSION_URL_FILE2));
    }

    private void assertTopology(final String input) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONParseException, JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, new Values(input));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"Record is indexed correctly\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]",
                        "[[1,\"NOTIFICATION\",{\"resource\":\"" + SOURCE_VERSION_URL_FILE2 + "\",\"info_text\":\"Record is indexed correctly\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
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

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private List selectSingle(List actualTuples, int index) {
        return Arrays.asList(actualTuples.get(index));
    }
}
