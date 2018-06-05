package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import com.mongodb.util.JSONParseException;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.*;
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
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, IndexingBolt.class, NotificationBolt.class, ValidationRevisionWriter.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, TaskStatusChecker.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*"})
public class IndexingTopologyTest extends TopologyTestHelper {

    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.INDEXING_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);

    @BeforeClass
    public static void prepareTopology() {
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
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.INDEXING_BOLT, new IndexingBolt(readProperties("indexing.properties"))).shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, new ValidationRevisionWriter(MCS_URL, IndexingTopology.SUCCESS_MESSAGE)).shuffleGrouping(TopologyHelper.INDEXING_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
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
    public final void shouldTestSuccessfulExecution() throws Exception {
        //mocking
        IndexerFactory indexerFactory = Mockito.mock(IndexerFactory.class);
        Indexer indexer = Mockito.mock(Indexer.class);
        PowerMockito.whenNew(IndexerFactory.class).withAnyArguments().thenReturn(indexerFactory);
        Mockito.when(indexerFactory.getIndexer()).thenReturn(indexer);
        //

        when(fileServiceClient.getFile(SOURCE_VERSION_URL)).thenReturn(new ByteArrayInputStream(new byte[]{'a', 'b', 'c'}));
        //
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);
        //
        //given
        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(1);
        Map<String, String> taskParameters = new HashMap<>();
        taskParameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        taskParameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.AUTHORIZATION_HEADER);
        taskParameters.put(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        taskParameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, SOURCE_VERSION_URL);
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


    protected void mockRevisionServiceClient() throws Exception {
        revisionServiceClient = Mockito.mock(RevisionServiceClient.class);
        PowerMockito.whenNew(RevisionServiceClient.class).withAnyArguments().thenReturn(revisionServiceClient);
    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONParseException, JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"Record is indexed correctly\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");

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
