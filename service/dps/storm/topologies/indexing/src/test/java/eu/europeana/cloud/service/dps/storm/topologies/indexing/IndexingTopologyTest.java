package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS;
import static eu.europeana.cloud.service.dps.test.TestConstants.MCS_URL;
import static eu.europeana.cloud.service.dps.test.TestConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static eu.europeana.cloud.service.dps.test.TestConstants.TEST_END_BOLT;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.CassandraDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.io.IndexingRevisionWriter;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TestInspectionBolt;
import eu.europeana.cloud.service.dps.storm.utils.TestSpout;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.indexing.IndexerPool;
import java.io.ByteArrayInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
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

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, IndexingBolt.class, NotificationBolt.class, IndexingRevisionWriter.class,
        CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class,
        CassandraTaskErrorsDAO.class,  CassandraNodeStatisticsDAO.class, WriteRecordBolt.class, ReadFileBolt.class,
        TaskStatusChecker.class, TaskStatusUpdater.class, TaskDiagnosticInfoDAO.class, CassandraDAO.class,
        HarvestedRecordsDAO.class, ProcessedRecordsDAO.class,UniqueIdentifierService.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*", "eu.europeana.cloud.test.CassandraTestInstance",
        "org.apache.logging.log4j.*","com.sun.org.apache.xerces.*","javax.xml.parsers.*","org.mockito.*",
        "javax.xml.*","org.springframework.util.*","com.ctc.wstx.*","com.sun.*"
})

public class IndexingTopologyTest extends TopologyTestHelper {

    private static final String AUTHORIZATION = "Authorization";

    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.INDEXING_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);

    private UISClient uisClient;

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
        mockUISClient();
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
        builder.setBolt(TopologyHelper.INDEXING_BOLT, new IndexingBolt(DbConnectionDetails.builder()
                .hosts("").port(1).keyspaceName("").userName("").password("").build()
                , readProperties("indexing.properties"), "uisLocation"))
                .shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, new IndexingRevisionWriter(MCS_URL, IndexingTopology.SUCCESS_MESSAGE)).shuffleGrouping(TopologyHelper.INDEXING_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));

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
        IndexerPool indexerPool = Mockito.mock(IndexerPool.class);
        PowerMockito.whenNew(IndexerPool.class).withAnyArguments().thenReturn(indexerPool);
        //

        when(fileServiceClient.getFile(SOURCE_VERSION_URL, AUTHORIZATION, PluginParameterKeys.AUTHORIZATION_HEADER)).thenReturn(new ByteArrayInputStream(new byte[]{'a', 'b', 'c'}));
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
        taskParameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE_VERSION_URL);
        taskParameters.put(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "TRUE");
        taskParameters.put(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS, "FALSE");
        taskParameters.put(PluginParameterKeys.PERFORM_REDIRECTS, "TRUE");
        taskParameters.put(PluginParameterKeys.DATASET_IDS_TO_REDIRECT_FROM, "dataset1, dataset2");
        taskParameters.put(PluginParameterKeys.HARVEST_DATE, DateHelper.format(Instant.now()));
        DateFormat dateFormat = new SimpleDateFormat(IndexingBolt.DATE_FORMAT, Locale.US);
        taskParameters.put(PluginParameterKeys.METIS_RECORD_DATE, dateFormat.format(new Date()));
        taskParameters.put(MESSAGE_PROCESSING_START_TIME_IN_MS, String.valueOf(Instant.now().toEpochMilli()));
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

    protected void mockUISClient() throws Exception {
        uisClient = Mockito.mock(UISClient.class);
        PowerMockito.whenNew(UISClient.class).withAnyArguments().thenReturn(uisClient);

        CloudId cloudId=new CloudId();
        cloudId.setId("abc");
        LocalId localId = new LocalId();
        localId.setRecordId("/1/afasd");
        localId.setProviderId("metis_junit");
        cloudId.setLocalId(localId);
        List<CloudId> cloudIds= Collections.singletonList(cloudId);
        when(uisClient.getRecordId(anyString())).thenReturn(new ResultSlice<>(null,cloudIds));
    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"Record is indexed correctly\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");

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
