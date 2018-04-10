package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

public class NotificationBoltTest extends CassandraTestBase {

    private OutputCollector collector;
    private NotificationBolt testedBolt;
    private CassandraTaskInfoDAO taskInfoDAO;
    private static final int PROCESSED_INTERVAL = 100;


    @Before
    public void setUp() throws Exception {
        collector = Mockito.mock(OutputCollector.class);
        testedBolt = new NotificationBolt(HOST, PORT, KEYSPACE, "", "");
        NotificationBolt.clearCache();

        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));

    }

    @Test
    public void testUpdateBasicInfoStateWithStartDateAndInfo() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 1;
        int expectedSize = 1;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        Date startTime = new Date();
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName, taskState, taskInfo, null, startTime, null);
        taskInfoDAO.insert(taskId, topologyName, expectedSize, containsElements, taskState.toString(), taskInfo, null, startTime, null, 0);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareUpdateTask(taskId, taskInfo, taskState, startTime));
        //when
        testedBolt.execute(tuple);
        //then
        TaskInfo result = taskInfoDAO.searchById(taskId);
        assertThat(result, notNullValue());
        assertThat(result, is(expectedTaskInfo));
    }

    @Test
    public void testUpdateBasicInfoStateWithFinishDateAndInfo() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 1;
        int expectedSize = 1;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        Date finishDate = new Date();
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName, taskState, taskInfo, null, null, finishDate);
        taskInfoDAO.insert(taskId, topologyName, expectedSize, containsElements, taskState.toString(), taskInfo, null, null, finishDate, 0);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareEndTask(taskId, taskInfo, taskState, finishDate));
        //when
        testedBolt.execute(tuple);
        //then
        TaskInfo result = taskInfoDAO.searchById(taskId);
        assertThat(result, notNullValue());
        assertThat(result, is(expectedTaskInfo));
    }


    @Test
    public void testSuccessfulNotificationFor101Tuples() throws Exception {
//given

        CassandraReportService cassandraReportService = new CassandraReportService(HOST, PORT, KEYSPACE, "", "");
        long taskId = 1;
        int expectedSize = 101;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        taskInfoDAO.insert(taskId, topologyName, expectedSize, 0, taskState.toString(), taskInfo, null, null, null, 0);
        String resource = "resource";
        States state = States.SUCCESS;
        String text = "text";
        String additionalInformation = "additionalInformation";
        String resultResource = "";
        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareUpdateTask(taskId, taskInfo, taskState, null));
        testedBolt.execute(setUpTuple);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(taskId, resource, state, text, additionalInformation, resultResource));
        TaskInfo beforeExecute = cassandraReportService.getTaskProgress(String.valueOf(taskId));
        testedBolt.execute(tuple);

        for (int i = 0; i < 99; i++) {
            testedBolt.execute(tuple);
        }

        TaskInfo afterOneHundredExecutions = cassandraReportService.getTaskProgress(String.valueOf(taskId));
        testedBolt.execute(tuple);
        assertEquals(beforeExecute.getProcessedElementCount(), 0);
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));

        assertEquals(afterOneHundredExecutions.getProcessedElementCount(), 100);
        assertThat(afterOneHundredExecutions.getState(), is(TaskState.CURRENTLY_PROCESSING));
    }


    @Test
    public void testNotificationProgressPercentage() throws Exception {
        //given
        CassandraReportService cassandraReportService = new CassandraReportService(HOST, PORT, KEYSPACE, "", "");
        long taskId = 1;
        int expectedSize = 330;
        int errors = 5;
        int middle = (int)(Math.random() * expectedSize);
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        taskInfoDAO.insert(taskId, topologyName, expectedSize, 0, taskState.toString(), taskInfo, null, null, null, 0);
        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareUpdateTask(taskId, taskInfo, taskState, null));
        testedBolt.execute(setUpTuple);

        //when
        List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

        TaskInfo beforeExecute = cassandraReportService.getTaskProgress(String.valueOf(taskId));
        TaskInfo middleExecute = null;

        for (int i = 0; i < tuples.size(); i++) {
            testedBolt.execute(tuples.get(i));
            if (i == middle - 1) {
                middleExecute = cassandraReportService.getTaskProgress(String.valueOf(taskId));
            }
        }

        TaskInfo afterExecute = cassandraReportService.getTaskProgress(String.valueOf(taskId));

        //then
        assertEquals(beforeExecute.getProcessedElementCount(), 0);
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        assertEquals(beforeExecute.getProcessedPercentage(), 0);

        if (middleExecute != null) {
            assertEquals(middleExecute.getProcessedElementCount(), (middle / PROCESSED_INTERVAL) * PROCESSED_INTERVAL);
            assertThat(middleExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
            assertEquals(middleExecute.getProcessedPercentage(), 100 * ((middle / PROCESSED_INTERVAL) * PROCESSED_INTERVAL)/ expectedSize);
        }

        assertEquals(afterExecute.getProcessedElementCount(), (expectedSize / PROCESSED_INTERVAL) * PROCESSED_INTERVAL);
        assertThat(afterExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        assertEquals(afterExecute.getProcessedPercentage(), 100 * ((afterExecute.getProcessedElementCount() / PROCESSED_INTERVAL) * PROCESSED_INTERVAL)/ expectedSize);
    }


    @Test
    public void testNotificationForErrors() throws Exception {
        //given
        CassandraReportService cassandraReportService = new CassandraReportService(HOST, PORT, KEYSPACE, "", "");
        long taskId = 1;
        int expectedSize = 20;
        int errors = 9;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        taskInfoDAO.insert(taskId, topologyName, expectedSize, 0, taskState.toString(), taskInfo, null, null, null, 0);
        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareUpdateTask(taskId, taskInfo, taskState, null));
        testedBolt.execute(setUpTuple);

        //when
        List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

        TaskInfo beforeExecute = cassandraReportService.getTaskProgress(String.valueOf(taskId));

        for (Tuple tuple : tuples) {
            testedBolt.execute(tuple);
        }

        TaskErrorsInfo errorsInfo = cassandraReportService.getGeneralTaskErrorReport(String.valueOf(taskId), 0);

        //then
        assertEquals(beforeExecute.getProcessedElementCount(), 0);
        assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
        assertEquals(beforeExecute.getErrors(), 0);

        assertEquals(errorsInfo.getErrors().size(), 1);
        assertEquals(errorsInfo.getErrors().get(0).getOccurrences(), errors);
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
            result.add(createTestTuple(NotificationTuple.prepareNotification(taskId, resource + String.valueOf(i), States.ERROR, text, additionalInformation, resultResource)));
        }

        while (result.size() < size) {
            result.add(createTestTuple(NotificationTuple.prepareNotification(taskId, resource + String.valueOf(result.size()), States.SUCCESS, text, additionalInformation, resultResource)));
        }
        return result;
    }

    private TaskInfo createTaskInfo(long taskId, int containElement, String topologyName, TaskState state, String info, Date sentTime, Date startTime, Date finishTime) {
        TaskInfo expectedTaskInfo = new TaskInfo(taskId, topologyName, state, info, sentTime, startTime, finishTime);
        expectedTaskInfo.setExpectedSize(containElement);
        return expectedTaskInfo;
    }


    private Tuple createTestTuple(NotificationTuple tuple) {
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