package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
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

import java.io.IOException;
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
    public void shouldSetTaskStateTo_Processed_ForTaskWithOneRecordAndIncrementalIndexing() throws Exception {
        long taskId = 1;
        prepareDpsTask(taskId, "true");

        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_Processed_ForTaskWithMultipleRecordsAndIncrementalIndexing() throws Exception {
        long taskId = 1;
        prepareDpsTask(taskId, "true");

        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(10));
        assertEquals(10, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_ReadyForPostprocessing_ForTaskWithOneRecordAndFullIndexing() throws Exception {
        long taskId = 1;
        prepareDpsTask(taskId, null);

        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.READY_FOR_POST_PROCESSING, taskProgress.getState());
    }

    @Test
    public void shouldSetTaskStateTo_ReadyForPostProcessing_ForTaskWithMultipleRecordsAndFullIndexing() throws Exception {
        long taskId = 1;
        prepareDpsTask(taskId, "false");


        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(5));
        assertEquals(5, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.READY_FOR_POST_PROCESSING, taskProgress.getState());
    }

    @Test
    public void shouldDropTheTaskInCaseOfNonParseableAndNonIncrementalDpsTask() throws Exception {
        long taskId = 1;
        prepareDpsTaskThatHaveNonParseableTaskInformation(taskId, "false");

        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.DROPPED, taskProgress.getState());
    }

    @Test
    public void shouldDropTheTaskInCaseOfNonParseableAndIncrementalDpsTask() throws Exception {
        long taskId = 1;
        prepareDpsTaskThatHaveNonParseableTaskInformation(taskId, "true");

        Tuple tuple = createNotificationTuple(taskId);
        testedBolt.execute(tuple);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(TaskState.DROPPED, taskProgress.getState());
    }

    private void prepareDpsTask(long taskId,String incrementalFlagValue) throws IOException {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.INCREMENTAL_INDEXING, incrementalFlagValue);
        String taskJSON = dpsTask.toJSON();
        taskInfoDAO.insert(
                TaskInfo.builder()
                        .id(taskId)
                        .topologyName("sample")
                        .state(TaskState.QUEUED)
                        .stateDescription("")
                        .expectedRecordsNumber(1)
                        .definition(taskJSON)
                        .build());
    }

    private void prepareDpsTaskThatHaveNonParseableTaskInformation(long taskId,String incrementalFlagValue) {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(PluginParameterKeys.INCREMENTAL_INDEXING, incrementalFlagValue);
        taskInfoDAO.insert(
                TaskInfo.builder()
                        .id(taskId)
                        .topologyName("sample")
                        .state(TaskState.QUEUED)
                        .expectedRecordsNumber(1)
                        .definition("")
                        .definition("nonParseableTaskInformations")
                        .build());
    }

    private void createBolt() {
        testedBolt = new IndexingNotificationBolt(CassandraTestBase.HOST, CassandraTestInstance.getPort(), CassandraTestBase.KEYSPACE, "", "");

        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
    }

    private Tuple createNotificationTuple(long taskId) {
        String resource = "resource" + ++resourceCounter;
        return createNotificationTuple(taskId, resource);
    }

    private Tuple createNotificationTuple(long taskId, String resource) {
        String text = "text";
        String additionalInformation = "additionalInformation";
        String resultResource = "";
        return createTestTuple(NotificationTuple.prepareNotification(taskId, false, resource, RecordState.SUCCESS, text,
                additionalInformation, resultResource, 1L));
    }

    private Tuple createTestTuple(NotificationTuple tuple) {

        String recordId = (String) tuple.getParameters().get(NotificationParameterKeys.RESOURCE);

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
        return new TupleImpl(topologyContext, testValue, "BoltTest", 1, "");
    }

}