package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.service.ReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

public class IndexingNotificationBoltTest extends CassandraTestBase {

    public static final String USER_NAME = "";
    public static final String PASSWORD = "";

    private CassandraTaskInfoDAO taskInfoDAO;
    private OutputCollector collector;
    private ReportService reportService;
    private ProcessedRecordsDAO processedRecordsDAO;
    private NotificationBolt testedBolt;
    private int resourceCounter = 0;

    @Before
    public void setUp() throws Exception {
        collector = Mockito.mock(OutputCollector.class);
        createBolt();
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(CassandraTestBase.HOST, CassandraTestInstance.getPort(), CassandraTestBase.KEYSPACE, "", ""));
        reportService = new ReportService(CassandraTestBase.HOST, CassandraTestInstance.getPort(), CassandraTestBase.KEYSPACE, USER_NAME, PASSWORD);

        CassandraConnectionProvider db = new CassandraConnectionProvider(CassandraTestBase.HOST, CassandraTestInstance.getPort(), CassandraTestBase.KEYSPACE, USER_NAME, PASSWORD);
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(db);
    }

    @Test
    public void shouldSetTaskStateTo_ReadyForPostProcessing_ForTaskWithOneRecordAndIncrementalIndexing() throws Exception {
        long taskId = 1;
        taskInfoDAO.insert(taskId, "sample", 1, 0, TaskState.CURRENTLY_PROCESSING.toString(), "", null, null, null, 0, null);

        Tuple tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedElementCount());
        assertEquals(TaskState.READY_FOR_POST_PROCESSING, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_ReadyForPostProcessing_ForTaskWithMultipleRecordsAndIncrementalIndexing() throws Exception {
        long taskId = 1;
        taskInfoDAO.insert(taskId, "sample", 10, 0, TaskState.CURRENTLY_PROCESSING.toString(), "", null, null, null, 0, null);

        Tuple tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, true);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(10));
        assertEquals(10, taskProgress.getProcessedElementCount());
        assertEquals(TaskState.READY_FOR_POST_PROCESSING, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_Processed_ForTaskWithOneRecordAndFullIndexing() throws Exception {
        long taskId = 1;
        taskInfoDAO.insert(taskId, "sample", 1, 0, TaskState.CURRENTLY_PROCESSING.toString(), "", null, null, null, 0, null);

        Tuple tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedElementCount());
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_Processed_ForTaskWithMultipleRecordsAndFullIndexing() throws Exception {
        long taskId = 1;
        taskInfoDAO.insert(taskId, "sample", 5, 0, TaskState.CURRENTLY_PROCESSING.toString(), "", null, null, null, 0, null);

        Tuple tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId, false);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(5));
        assertEquals(5, taskProgress.getProcessedElementCount());
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
    }


    private void createBolt() {
        testedBolt = new IndexingNotificationBolt(CassandraTestBase.HOST, CassandraTestInstance.getPort(), CassandraTestBase.KEYSPACE, "", "");

        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
    }

    private Tuple createNotificationTuple(long taskId, boolean inIncrementalProcessing) {
        String resource = "resource" + ++resourceCounter;
        return createNotificationTuple(taskId, resource, inIncrementalProcessing);
    }

    private Tuple createNotificationTuple(long taskId, String resource, boolean inIncrementalProcessing) {
        String text = "text";
        String additionalInformation = "additionalInformation";
        String resultResource = "";
        return createTestTuple(NotificationTuple.prepareNotification(taskId, resource, RecordState.SUCCESS, text, additionalInformation, resultResource, 1L), inIncrementalProcessing);
    }

    private Tuple createTestTuple(NotificationTuple tuple, boolean inIncrementalProcessing) {

        String recordId = (String) tuple.getParameters().get(NotificationParameterKeys.RESOURCE);
        if (inIncrementalProcessing) {
            tuple.addParameter(PluginParameterKeys.INCREMENTAL_INDEXING, "true");
        } else {
            tuple.addParameter(PluginParameterKeys.INCREMENTAL_INDEXING, "false");
        }

        if (recordId != null) {
            processedRecordsDAO.insert(tuple.getTaskId(), recordId, 1, "", "",
                    RecordState.QUEUED.toString(), "", "");
        }
        Values testValue = tuple.toStormTuple();
        TopologyBuilder builder = new TopologyBuilder();
        @SuppressWarnings("unchecked")
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return NotificationTuple.getFields();
            }
        };
        return new TupleImpl(topologyContext, testValue, 1, "");
    }

}