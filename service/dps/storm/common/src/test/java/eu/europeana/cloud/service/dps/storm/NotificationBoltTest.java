package eu.europeana.cloud.service.dps.storm;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class NotificationBoltTest extends CassandraTestBase {

    private OutputCollector collector;
    private NotificationBolt testedBolt;
    private CassandraTaskInfoDAO taskInfoDAO;


    @Before
    public void setUp() throws Exception {
        collector = Mockito.mock(OutputCollector.class);
        testedBolt = new NotificationBolt(HOST, PORT, KEYSPACE, "", "");
        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
        taskInfoDAO = new CassandraTaskInfoDAO(new CassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));
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
        taskInfoDAO.insert(taskId, topologyName, expectedSize, containsElements, taskState.toString(), taskInfo, null, startTime, null);
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
        taskInfoDAO.insert(taskId, topologyName, expectedSize, containsElements, taskState.toString(), taskInfo, null, null, finishDate);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareEndTask(taskId, taskInfo, taskState, finishDate));
        //when
        testedBolt.execute(tuple);
        //then
        TaskInfo result = taskInfoDAO.searchById(taskId);
        assertThat(result, notNullValue());
        assertThat(result, is(expectedTaskInfo));
    }

    @Test
    public void testSuccessfulNotificationTuple() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 1;
        int expectedSize = 1;
        String topologyName = null;
        TaskState taskState = TaskState.CURRENTLY_PROCESSING;
        String taskInfo = "";
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName, TaskState.PROCESSED, taskInfo, null, null, null);
        taskInfoDAO.insert(taskId, topologyName, expectedSize, containsElements, taskState.toString(), taskInfo, null, null, null);
        String resource = "resource";
        States state = States.SUCCESS;
        String text = "text";
        String additionalInformation = "additionalInformations";
        String resultResource = "";
        expectedTaskInfo.addSubtask(new SubTaskInfo(1, resource, state, text, additionalInformation, resultResource));

        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareUpdateTask(taskId, taskInfo, taskState, null));
        testedBolt.execute(setUpTuple);

        final Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(taskId, resource, state, text, additionalInformation, resultResource));
        //when
        testedBolt.execute(tuple);
        //then
        TaskInfo result = taskInfoDAO.searchByIdWithSubtasks(taskId);
        assertThat(result, notNullValue());
        result.setContainsElements(result.getSubtasks().size());
        assertThat(result, is(expectedTaskInfo));
    }

    private TaskInfo createTaskInfo(long taskId, int containElement, String topologyName, TaskState state, String info, Date sentTime, Date startTime, Date finishTime) {
        TaskInfo expectedTaskInfo = new TaskInfo(taskId, topologyName, state, info, sentTime, startTime, finishTime);
        expectedTaskInfo.setContainsElements(containElement);
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