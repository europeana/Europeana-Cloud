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
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void testSuccessfulBasicInfoTuple() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 2;
        String topologyName = "";
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareBasicInfo(taskId, containsElements));
        //when
        testedBolt.execute(tuple);
        //then
        List<TaskInfo> result = taskInfoDAO.searchByIdWithSubtasks(taskId);
        assertThat(result, notNullValue());
        assertThat(result.size(), is(equalTo(1)));
        TaskInfo info = result.get(0);
        assertThat(info, is(expectedTaskInfo));
    }

    @Test
    public void testSuccessfulBasicInfoAndNotificationTuple() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 2;
        String topologyName = "";
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName);

        String resource = "resource";
        States state = States.SUCCESS;
        String text = "text";
        String additionalInformations = "additionalInformations";
        expectedTaskInfo.addSubtask(new SubTaskInfo(resource, state, text, additionalInformations));
        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareBasicInfo(taskId, containsElements));
        testedBolt.execute(setUpTuple);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(taskId, resource, state, text, additionalInformations));
        //when
        testedBolt.execute(tuple);
        //then
        List<TaskInfo> result = taskInfoDAO.searchByIdWithSubtasks(taskId);
        assertThat(result, notNullValue());
        assertThat(result.size(), is(equalTo(1)));
        TaskInfo info = result.get(0);
        expectedTaskInfo.equals(info);
        assertThat(info, is(expectedTaskInfo));
    }

    @Test
    public void testSuccessfulNotificationTuple() throws Exception {
        //given
        long taskId = 1;
        int containsElements = 2;
        String topologyName = "";
        TaskInfo expectedTaskInfo = createTaskInfo(taskId, containsElements, topologyName);

        String resource = "resource";
        States state = States.SUCCESS;
        String text = "text";
        String additionalInformations = "additionalInformations";
        expectedTaskInfo.addSubtask(new SubTaskInfo(resource, state, text, additionalInformations));
        final Tuple setUpTuple = createTestTuple(NotificationTuple.prepareBasicInfo(taskId, containsElements));
        testedBolt.execute(setUpTuple);
        final Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(taskId, resource, state, text, additionalInformations));
        //when
        testedBolt.execute(tuple);
        //then
        List<TaskInfo> result = taskInfoDAO.searchByIdWithSubtasks(taskId);
        assertThat(result, notNullValue());
        assertThat(result.size(), is(equalTo(1)));
        TaskInfo info = result.get(0);
        expectedTaskInfo.equals(info);
        assertThat(info, is(expectedTaskInfo));
    }

    private TaskInfo createTaskInfo(long taskId, int containsElements, String topologyName) {
        TaskInfo expectedTaskInfo = new TaskInfo(taskId, topologyName);
        expectedTaskInfo.setContainsElements(containsElements);
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