package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.AckTracker;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RemovePermissionsToFileBolt.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class RemovePermissionsToFileBoltTest {

    private static final String PROPER_MCS_FILE_URL = "http://localhost:8080/mcs/records/RJUK2YK567SH75DW5GXVH5MXUUGVGX2QUKZC4L6HTPQPZEDE3Q5Q/representations/RepName_0000001/versions/5f860750-8c36-11e5-80b8-0a0027000001/files/cb8f8dc1-78cb-4cb3-a6ad-84e34aa6ca91";
    private static final String MALFORMED_MCS_FILE_URL = "MALFORMED_MCS_FILE_URL";

    private RecordServiceClient recordServiceClient;
    private FeederSpout spout;
    private AckTracker tracker;

    public RemovePermissionsToFileBoltTest() {

    }


    @Before
    public void setUp() throws Exception {
        dirtyMockZookeeperKS();
        dirtyMockRecordSC();
    }

    @Test
    public void successfullyRemovingPermissions() throws Exception {
        //given
        Mockito.doNothing().when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final List<Values> data = prepareInputData(PROPER_MCS_FILE_URL);
        //when
        testOnLocalCluster(data);
        //then
        assertThat(tracker.getNumAcks(), is(1));

    }


    @Test
    public void throwsMalformedExceptionOnGrantingPermissions() throws Exception {
        //given
        Mockito.doNothing().when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final List<Values> data = prepareInputData(MALFORMED_MCS_FILE_URL);
        //when
        testOnLocalCluster(data);
        //then
        assertThat(tracker.getNumAcks(), is(0));
    }

    @Test
    public void throwsMCSExceptionOnGrantingPermissions() throws Exception {
        //given
        Mockito.doThrow(new MCSException()).when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        final List<Values> data = prepareInputData(PROPER_MCS_FILE_URL);
        //when
        testOnLocalCluster(data);
        //then
        assertThat(tracker.getNumAcks(), is(0));
    }


    private void testOnLocalCluster(final List<Values> data) {
        Testing.withTrackedCluster(new TestJob() {
            @Override
            public void run(ILocalCluster cluster)
                    throws IOException, AlreadyAliveException, InvalidTopologyException {
                TrackedTopology topology = createTestTopology(cluster);
                submitTopology(cluster, topology);
                for (Values tuple : data) {
                    spout.feed(tuple);
                    Testing.trackedWait(topology, 1, 60000);
                }

            }
        });
    }


    private TrackedTopology createTestTopology(ILocalCluster cluster) {
        tracker = new AckTracker();
        spout = new FeederSpout(StormTaskTuple.getFields());
        spout.setAckFailDelegate(tracker);

        //build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", spout);
        RemovePermissionsToFileBolt bolt = new RemovePermissionsToFileBolt("http://localhost:8080/mcs/", "admin", "admin");
        builder.setBolt("removePermissionsToFileBolt", bolt)
                .shuffleGrouping("testSpout");
        return Testing.mkTrackedTopology(cluster, builder.createTopology());
    }

    private void submitTopology(ILocalCluster cluster, TrackedTopology tt) throws AlreadyAliveException, InvalidTopologyException {
        //topology config
        Config config = new Config();
        config.setNumWorkers(1);
        config.setDebug(true);
        cluster.submitTopology("testTopology", config, tt.getTopology());
    }

    private void dirtyMockZookeeperKS() throws Exception {
        ZookeeperKillService zookeeperKillService = Mockito.mock(ZookeeperKillService.class);
        Mockito.when(zookeeperKillService.hasKillFlag(anyString(), anyLong())).thenReturn(false);
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zookeeperKillService);

    }

    private List<Values> prepareInputData(String fileUrl) {
        int i = 1;
        List<Values> result = new ArrayList<>();
        Map<String, String> taskParams = new HashMap<>();
        taskParams.put(PluginParameterKeys.TASK_SUBMITTER_NAME, "user");
        taskParams.put(PluginParameterKeys.OUTPUT_URL, fileUrl);
        result.add(new StormTaskTuple(i, "task", "fileUrl", null, taskParams).toStormTuple());
        return result;
    }

    private void dirtyMockRecordSC() throws Exception {
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);
    }
}