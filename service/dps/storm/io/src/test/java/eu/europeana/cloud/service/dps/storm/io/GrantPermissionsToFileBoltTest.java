package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(PowerMockRunner.class)
@PrepareForTest(GrantPermissionsToFileBolt.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class GrantPermissionsToFileBoltTest {

    private static final String PROPER_MCS_FILE_URL = "http://localhost:8080/mcs/records/RJUK2YK567SH75DW5GXVH5MXUUGVGX2QUKZC4L6HTPQPZEDE3Q5Q/representations/RepName_0000001/versions/5f860750-8c36-11e5-80b8-0a0027000001/files/cb8f8dc1-78cb-4cb3-a6ad-84e34aa6ca91";
    private static final String MALFORMED_MCS_FILE_URL = "MALFORMED_MCS_FILE_URL";

    private RecordServiceClient recordServiceClient;
    private GrantPermissionsToFileBolt testedBolt;


    public GrantPermissionsToFileBoltTest() {
    }

    @Mock
    private OutputCollector collector;


    @Before
    public void setUp() throws Exception {
        dirtyMockZookeeperKS();
        dirtyMockRecordSC();
        testedBolt = new GrantPermissionsToFileBolt("http://localhost:8080/mcs/", "admin", "admin");
        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        testedBolt.prepare(boltConfig, null, collector);
    }


    @Test
    public void successfullyGrantingPermissions() throws Exception {
        //given
        Mockito.doNothing().when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final Tuple tuple = createTestTuple(PROPER_MCS_FILE_URL);
        //when
        testedBolt.execute(tuple);
        //then
        assertSuccessEmit(tuple);
    }


    @Test
    public void throwsMalformedExceptionOnGrantingPermissions() throws Exception {
        //given
        Mockito.doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final Tuple tuple = createTestTuple(MALFORMED_MCS_FILE_URL);
        final NotificationTuple expectedNotification = NotificationTuple.prepareNotification(1,
                "fileUrl", NotificationTuple.States.ERROR, "Url to file is malformed. Permissions will not be granted on: MALFORMED_MCS_FILE_URLno protocol: " +
                        "MALFORMED_MCS_FILE_URL", "{OUTPUT_URL=MALFORMED_MCS_FILE_URL, TASK_SUBMITTER_NAME=user}");
        //when
        testedBolt.execute(tuple);
        //then
        assertErrorEmit(tuple, expectedNotification);
    }


    @Test
    public void throwsMCSExceptionOnGrantingPermissions() throws Exception {
        //given
        Mockito.doThrow(new MCSException()).when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final Tuple tuple = createTestTuple(PROPER_MCS_FILE_URL);
        final NotificationTuple expectedNotification = NotificationTuple.prepareNotification(1,
                "fileUrl", NotificationTuple.States.ERROR, "There was exception while trying to granted permissions on: " +
                        "http://localhost:8080/mcs/records/RJUK2YK567SH75DW5GXVH5MXUUGVGX2QUKZC4L6HTPQPZEDE3Q5Q/representations/RepName_0000001/versions/5f860750-8c36-11e5-80b8-0a0027000001/files/cb8f8dc1-78cb-4cb3-a6ad-84e34aa6ca91null",
                "{OUTPUT_URL=http://localhost:8080/mcs/records/RJUK2YK567SH75DW5GXVH5MXUUGVGX2QUKZC4L6HTPQPZEDE3Q5Q/representations/RepName_0000001/versions/5f860750-8c36-11e5-80b8-0a0027000001/files/cb8f8dc1-78cb-4cb3-a6ad-84e34aa6ca91, " +
                        "TASK_SUBMITTER_NAME=user}");
        //when
        testedBolt.execute(tuple);
        //then
        assertErrorEmit(tuple, expectedNotification);

    }

    private void assertSuccessEmit(Tuple tuple) {
        verify(collector, times(1)).ack(tuple);
        verify(collector, times(1)).emit(eq(tuple), any(Values.class));
    }

    private void assertErrorEmit(Tuple tuple, NotificationTuple expectedNotification) {
        verify(collector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), eq(expectedNotification.toStormTuple()));
        verify(collector, times(0)).ack(tuple);
    }

    private Tuple createTestTuple(String fileUrl) {
        Map<String, String> taskParams = new HashMap<>();
        taskParams.put(PluginParameterKeys.TASK_SUBMITTER_NAME, "user");
        taskParams.put(PluginParameterKeys.OUTPUT_URL, fileUrl);
        TopologyBuilder builder = new TopologyBuilder();
        @SuppressWarnings("unchecked")
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return StormTaskTuple.getFields();
            }
        };
        Values testValue = new StormTaskTuple(1, "task", "fileUrl", null, taskParams).toStormTuple();
        return new TupleImpl(topologyContext, testValue, 1, "");
    }

    private void dirtyMockZookeeperKS() throws Exception {
        ZookeeperKillService zookeeperKillService = Mockito.mock(ZookeeperKillService.class);
        Mockito.when(zookeeperKillService.hasKillFlag(anyString(), anyLong())).thenReturn(false);
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zookeeperKillService);

    }


    private void dirtyMockRecordSC() throws Exception {
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);
    }
}