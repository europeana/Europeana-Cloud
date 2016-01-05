package eu.europeana.cloud;


import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest(ReadFileBolt.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ICSTopologyTest {


    private final int NUM_WORKERS = 2;
    private FileServiceClient fileServiceClient;


    public ICSTopologyTest() {

    }


    @Before
    public void setUp() throws Exception {
        dirtyMockZookeeperKS();
        dirtyMockRecordSC();
        dirtyMockFileSC();
    }


    @Test
    public void testBasicTopology() throws MCSException, IOException {
        //given
        when(fileServiceClient.getFile(anyString())).thenReturn(new ByteArrayInputStream("testContent".getBytes()));
        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"http://localhost:8080/mcs/records/NFOMD7VJIYPGQ7R75Q74QSBLLVFNKIVF4NCU2DDOBF4RJLUGWNRA/representations/RepName_0000001/versions/81114100-a969-11e5-9c25-0a0027000001/files/d1e576ad-cb55-40c0-a4b6-aa2f87ec4db2\"]}," +
                "\"parameters\":" +
                "{\"XSLT_URL\":\"http://localhost:8080/sample_xslt.xslt\"}," +
                "\"taskId\":3958147042249556511," +
                "\"taskName\":\"taskName\"}";
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new TestSpaut(), 1);
                ReadFileBolt retrieveFileBolt = new ReadFileBolt("", "", "");
                builder.setBolt("2", new ParseTaskBolt()).globalGrouping("1");
                builder.setBolt("3", retrieveFileBolt).globalGrouping("2");
                StormTopology topology = builder.createTopology();

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("1",  new Values(input));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(NUM_WORKERS);
                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                //when
                Map result = Testing.completeTopology(cluster, topology,
                        completeTopologyParam);

                //then
                List printIn = Testing.readTuples(result, "1");
                prettyPrintJSON(printIn);
                printIn = Testing.readTuples(result, "2");
                prettyPrintJSON(printIn);

            }

        });
    }

    private void prettyPrintJSON(List printIn) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(printIn);
        System.out.println(json);
    }

    private MkClusterParam prepareMKClusterParm() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        int SUPERVISORS = 4;
        mkClusterParam.setSupervisors(SUPERVISORS);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }


    private void dirtyMockZookeeperKS() throws Exception {
        ZookeeperKillService zookeeperKillService = Mockito.mock(ZookeeperKillService.class);
        when(zookeeperKillService.hasKillFlag(anyString(), anyLong())).thenReturn(false);
        PowerMockito.whenNew(ZookeeperKillService.class).withAnyArguments().thenReturn(zookeeperKillService);

    }

    private void dirtyMockRecordSC() throws Exception {
        RecordServiceClient recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);
    }

    private void dirtyMockFileSC() throws Exception {
        fileServiceClient = Mockito.mock(FileServiceClient.class);
        PowerMockito.whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileServiceClient);
    }
}