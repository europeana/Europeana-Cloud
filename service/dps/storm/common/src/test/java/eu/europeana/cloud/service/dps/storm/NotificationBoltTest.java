package eu.europeana.cloud.service.dps.storm;


import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.service.ReportService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
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

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class NotificationBoltTest extends CassandraTestBase {

    public static final String USER_NAME = "";
    public static final String PASSWORD = "";
    public static final String RESOURCE_1 = "resource1";
    public static final String RESOURCE_2 = "resource2";
    public static final String RESOURCE_3 = "resource3";
    public static final String RESOURCE_4 = "resource4";
    public static final String RESOURCE_5 = "resource5";
    public static final String RESOURCE_6 = "resource6";
    private static final String TOPOLOGY_NAME = "test_topology";
    public static final int TASK_ID = 1;
    private OutputCollector collector;
    private NotificationBolt testedBolt;
    private CassandraTaskInfoDAO taskInfoDAO;
    private ReportService reportService;
    private CassandraSubTaskInfoDAO subtaskDAO;
    private ProcessedRecordsDAO processedRecordsDAO;
    private CassandraTaskErrorsDAO cassandraTaskErrorsDAO;
    private int resourceCounter=0;

    @Before
    public void setUp() throws Exception {
        collector = Mockito.mock(OutputCollector.class);
        createBolt();
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, "", ""));
        reportService = new ReportService(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER_NAME, PASSWORD);

        CassandraConnectionProvider db=new CassandraConnectionProvider(HOST,CassandraTestInstance.getPort(),KEYSPACE,USER_NAME,PASSWORD);
        subtaskDAO = CassandraSubTaskInfoDAO.getInstance(db);
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(db);
        cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(db);
    }

    private void createBolt() {
        testedBolt = new NotificationBolt(HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");

        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
    }


    @Test
    public void shouldProperlyExecuteRegularTuple() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
        Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_1,
                RecordState.SUCCESS, "", "", 0));

        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(0, taskProgress.getIgnoredRecordsCount());
        assertEquals(0, taskProgress.getDeletedRecordsCount());
        assertEquals(0, taskProgress.getProcessedErrorsCount());
        assertEquals(0, taskProgress.getDeletedErrorsCount());
    }

    @Test
    public void shouldProperlyExecuteIgnoredRecordTuple() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
        NotificationTuple notificationTuple = NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_1,
                RecordState.SUCCESS, "", "", 0);
        notificationTuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
        Tuple tuple = createTestTuple(notificationTuple);

        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(0, taskProgress.getProcessedRecordsCount());
        assertEquals(1, taskProgress.getIgnoredRecordsCount());
        assertEquals(0, taskProgress.getDeletedRecordsCount());
        assertEquals(0, taskProgress.getProcessedErrorsCount());
        assertEquals(0, taskProgress.getDeletedErrorsCount());
    }

    @Test
    public void shouldProperlyExecuteDeletedRecordTuple() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
        Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(TASK_ID, true, RESOURCE_1,
                RecordState.SUCCESS, "", "", 0));

        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(0, taskProgress.getProcessedRecordsCount());
        assertEquals(0, taskProgress.getIgnoredRecordsCount());
        assertEquals(1, taskProgress.getDeletedRecordsCount());
        assertEquals(0, taskProgress.getProcessedErrorsCount());
        assertEquals(0, taskProgress.getDeletedErrorsCount());
    }

    @Test
    public void shouldProperlyExecuteFailedTuple() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
        Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_1,
                RecordState.ERROR, "", "", 0));

        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(1, taskProgress.getProcessedRecordsCount());
        assertEquals(0, taskProgress.getIgnoredRecordsCount());
        assertEquals(0, taskProgress.getDeletedRecordsCount());
        assertEquals(1, taskProgress.getProcessedErrorsCount());
        assertEquals(0, taskProgress.getDeletedErrorsCount());
    }

    @Test
    public void shouldProperlyExecuteFailedDeletedRecordTuple() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
        Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(TASK_ID, true, RESOURCE_1,
                RecordState.ERROR, "", "", 0));

        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(0, taskProgress.getProcessedRecordsCount());
        assertEquals(0, taskProgress.getIgnoredRecordsCount());
        assertEquals(1, taskProgress.getDeletedRecordsCount());
        assertEquals(0, taskProgress.getProcessedErrorsCount());
        assertEquals(1, taskProgress.getDeletedErrorsCount());
    }

    @Test
    public void verifyOnlyOneNotificationForRepeatedRecord() throws Exception {
        long taskId = 1;
        insertTaskToDB(taskId, null, 10, TaskState.CURRENTLY_PROCESSING, "");

        Tuple tuple = createNotificationTuple(taskId, RecordState.SUCCESS);
        testedBolt.execute(tuple);
        testedBolt.execute(tuple);

        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf(taskId));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + taskId, 0, 100);
        assertThat(notifications, hasSize(1));
        assertEquals(1, taskProgress.getProcessedRecordsCount());
    }


    @Test
    public void testSuccessfulNotificationFor101Tuples() throws Exception {
//given


        long taskId = 1;
        int expectedSize = 101;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

        TaskInfo beforeExecute = reportService.getTaskProgress(String.valueOf(taskId));
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

        for (int i = 0; i < 98; i++) {
            testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        }
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

        TaskInfo afterOneHundredExecutions = reportService.getTaskProgress(String.valueOf(taskId));
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        assertEquals(0, beforeExecute.getProcessedRecordsCount());
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));

        assertEquals(100, afterOneHundredExecutions.getProcessedRecordsCount());
        assertThat(afterOneHundredExecutions.getState(), is(TaskState.CURRENTLY_PROCESSING));
    }

    @Test
    public void testSuccessfulProgressUpdateAfterBoltRecreate() throws Exception {
        long taskId = 1;
        int expectedSize =4;
        String topologyName = "";
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        createBolt();
        //we will wait 5 second to be sure that notification bolt will update progress counter
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        Thread.sleep(5001);
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        TaskInfo info = reportService.getTaskProgress(String.valueOf(taskId));
        assertEquals(3, info.getProcessedRecordsCount());
        assertEquals(TaskState.CURRENTLY_PROCESSING, info.getState());
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

        info = reportService.getTaskProgress(String.valueOf(taskId));
        assertEquals(expectedSize, info.getProcessedRecordsCount());
        assertEquals(TaskState.PROCESSED, info.getState());

    }

    @Test
    public void testValidNotificationAfterBoltRecreate() throws Exception {
        long taskId = 1;
        int expectedSize = 2;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, 2, taskState, taskInfo);
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
        createBolt();
        testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

        assertEquals(expectedSize,subtaskDAO.getProcessedFilesCount(taskId));
    }

    @Test
    public void testValidErrorReportDataAfterBoltRecreate() throws Exception {
        long taskId = 1;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, 2, taskState, taskInfo);
        testedBolt.execute(createNotificationTuple(taskId, RecordState.ERROR, RESOURCE_1));
        createBolt();
        testedBolt.execute(createNotificationTuple(taskId, RecordState.ERROR, RESOURCE_2));


        assertEquals(2,subtaskDAO.getProcessedFilesCount(taskId));
        TaskErrorsInfo errorReport = reportService.getGeneralTaskErrorReport("" + taskId, 100);
        assertEquals(1 ,errorReport.getErrors().size());
        assertEquals(2 ,errorReport.getErrors().get(0).getOccurrences());
        TaskErrorsInfo specificReport = reportService.getSpecificTaskErrorReport("" + taskId, errorReport.getErrors().get(0).getErrorType(), 100);
        assertEquals(1,specificReport.getErrors().size());
        TaskErrorInfo specificReportErrorInfo = specificReport.getErrors().get(0);
        assertEquals("text",specificReportErrorInfo.getMessage());
        assertEquals(2,specificReportErrorInfo.getErrorDetails().size());
        assertEquals(RESOURCE_1, specificReportErrorInfo.getErrorDetails().get(0).getIdentifier());
        assertEquals(RESOURCE_2, specificReportErrorInfo.getErrorDetails().get(1).getIdentifier());
    }

    @Test
    public void shouldProperlyRestoreAllCountersAfterBoltRecreate() throws Exception {
        insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 6, TaskState.QUEUED, "");


        testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_1,
                RecordState.SUCCESS, "", "", 0)));
        NotificationTuple notificationTuple = NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_2,
                RecordState.SUCCESS, "", "", 0);
        notificationTuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
        testedBolt.execute(createTestTuple(notificationTuple));
        testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(TASK_ID, true, RESOURCE_3,
                RecordState.SUCCESS, "", "", 0)));
        testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_4,
                RecordState.ERROR, "", "", 0)));
        testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(TASK_ID, true, RESOURCE_5,
                RecordState.ERROR, "", "", 0)));
        createBolt();
        testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(TASK_ID, false, RESOURCE_6,
                RecordState.SUCCESS, "", "", 0)));


        TaskInfo taskProgress = reportService.getTaskProgress(String.valueOf((long) TASK_ID));
        List<SubTaskInfo> notifications = reportService.getDetailedTaskReport("" + (long) TASK_ID, 0, 100);
        assertThat(notifications, hasSize(6));
        assertEquals(TaskState.PROCESSED, taskProgress.getState());
        assertEquals(3, taskProgress.getProcessedRecordsCount());
        assertEquals(1, taskProgress.getIgnoredRecordsCount());
        assertEquals(2, taskProgress.getDeletedRecordsCount());
        assertEquals(1, taskProgress.getProcessedErrorsCount());
        assertEquals(1, taskProgress.getDeletedErrorsCount());
    }

    private Tuple createNotificationTuple(long taskId, RecordState state) {
        String resource = "resource"+ ++resourceCounter;
        return createNotificationTuple(taskId, state, resource);
    }

    private Tuple createNotificationTuple(long taskId, RecordState state, String resource) {
        String text = "text";
        String additionalInformation = "additionalInformation";
        String resultResource = "";
        return createTestTuple(NotificationTuple.prepareNotification(taskId, false, resource, state, text,
                additionalInformation, resultResource,1L));
    }


    @Test
    public void testNotificationProgressPercentage() throws Exception {
        //given
        ReportService reportService = new ReportService(HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");
        long taskId = 1;
        int expectedSize = 330;
        int errors = 5;
        int middle = (int) (Math.random() * expectedSize);
        String topologyName = "";
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

        //when
        List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

        TaskInfo beforeExecute = reportService.getTaskProgress(String.valueOf(taskId));
        TaskInfo middleExecute = null;

        for (var i = 0; i < tuples.size(); i++) {
            if(i == middle - 1){
                Thread.sleep(5001);
                testedBolt.execute(tuples.get(i));
                middleExecute = reportService.getTaskProgress(String.valueOf(taskId));
            }else{
                testedBolt.execute(tuples.get(i));
            }
        }

        TaskInfo afterExecute = reportService.getTaskProgress(String.valueOf(taskId));

        //then
        assertEquals(0, beforeExecute.getProcessedRecordsCount());
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        assertEquals(0, beforeExecute.getProcessedRecordsCount());

        if (middleExecute != null) {
            assertEquals(middleExecute.getProcessedRecordsCount(), (middle));
            assertThat(middleExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        }
        int totalProcessed = expectedSize;
        assertEquals(afterExecute.getProcessedRecordsCount(), totalProcessed+(expectedSize - totalProcessed) );
        assertThat(afterExecute.getState(), is(TaskState.PROCESSED));
    }


    @Test
    public void testNotificationForErrors() throws Exception {
        //given
        ReportService reportService = new ReportService(HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");
        long taskId = 1;
        int expectedSize = 20;
        int errors = 9;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

        //when
        List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

        TaskInfo beforeExecute = reportService.getTaskProgress(String.valueOf(taskId));

        for (Tuple tuple : tuples) {
            testedBolt.execute(tuple);
        }

        TaskErrorsInfo errorsInfo = reportService.getGeneralTaskErrorReport(String.valueOf(taskId), 0);

        //then
        assertEquals(0, beforeExecute.getProcessedRecordsCount());
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        assertEquals(0, beforeExecute.getProcessedErrorsCount());

        assertEquals(1, errorsInfo.getErrors().size());
        assertEquals(errorsInfo.getErrors().get(0).getOccurrences(), errors);
    }

    private void insertTaskToDB(long taskId, String topologyName, int expectedSize, TaskState state,
                                String info)
            throws NoHostAvailableException, QueryExecutionException {
        taskInfoDAO.insert(
                TaskInfo.builder()
                        .id(taskId)
                        .topologyName(topologyName)
                        .expectedRecordsNumber(expectedSize)
                        .state(state)
                        .stateDescription(info)
                        .build());

    }

    private List<Tuple> prepareTuples(long taskId, int size, int errors) {
        String resource = "resource";
        String text = "text";
        String additionalInformation = "additionalInformation";
        String resultResource = "";
        List<Tuple> result = new ArrayList<>();

        if (errors > size) {
            errors = size;
        }

        for (int i = 0; i < errors; i++) {
            result.add(createTestTuple(NotificationTuple.prepareNotification(taskId, false, resource + String.valueOf(i),
                    RecordState.ERROR, text, additionalInformation, resultResource, 1L)));
        }

        while (result.size() < size) {
            result.add(createTestTuple(NotificationTuple.prepareNotification(taskId, false,
                    resource + String.valueOf(result.size()), RecordState.SUCCESS, text, additionalInformation,
                    resultResource, 1L)));
        }
        return result;
    }

    private TaskInfo createTaskInfo(long taskId, int containElement, String topologyName, TaskState state, String info, Date sentTime, Date startTime, Date finishTime) {
        TaskInfo expectedTaskInfo = TaskInfo.builder()
                .id(taskId)
                .topologyName(topologyName)
                .state(state)
                .stateDescription(info)
                .sentTimestamp(sentTime)
                .startTimestamp(startTime)
                .finishTimestamp(finishTime)
                .build();
        expectedTaskInfo.setExpectedRecordsNumber(containElement);
        return expectedTaskInfo;
    }


    private Tuple createTestTuple(NotificationTuple tuple) {

        String recordId = (String)tuple.getParameters().get(NotificationParameterKeys.RESOURCE);
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