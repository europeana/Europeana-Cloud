package eu.europeana.cloud.service.dps.storm;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(org.mockito.runners.MockitoJUnitRunner.class)
//@ContextConfiguration(locations = {"classpath:/default-context.xml"})

public class NotificationBoltTest extends CassandraTestBase {

    @Mock
    private OutputCollector collector;

    private NotificationBolt testedBolt;

    @Before
    public void setUp() throws Exception {
        //dirtyMockZookeeperKS();
        //dirtyMockRecordSC();
        testedBolt = new NotificationBolt(CassandraTestBase.HOST, PORT, KEYSPACE, "", "");
        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
    }

    @Test
    public void throwsMCSExceptionOnGrantingPermissions() throws Exception {
        //given

        final Tuple tuple = createTestTuple(NotificationTuple.prepareBasicInfo(1, 2));
        final NotificationTuple expectedNotification = NotificationTuple.prepareBasicInfo(1, 2);
        //when
        testedBolt.execute(tuple);
        //then
        assertSuccessEmit(tuple, expectedNotification);

    }

    private void assertSuccessEmit(Tuple tuple, NotificationTuple tuple2) {
        verify(collector, times(1)).ack(tuple);
        verify(collector, times(1)).emit(eq(tuple2.toStormTuple()));
    }

    private void assertErrorEmit(Tuple tuple, NotificationTuple expectedNotification) {
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), eq(expectedNotification.toStormTuple()));
        verify(collector, times(0)).ack(tuple);
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