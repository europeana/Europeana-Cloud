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
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.EndBolt;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.GrantPermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RemovePermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterService;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterServiceImpl;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.eclipse.persistence.internal.jaxb.many.MapEntry;
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
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, IcBolt.class, WriteRecordBolt.class, GrantPermissionsToFileBolt.class, RemovePermissionsToFileBolt.class, EndBolt.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ICSTopologyTest {


    private static final String WRITE_RECORD_BOLT = "writeRecordBolt";
    private final int NUM_WORKERS = 2;
    private final String SPOUT = "spout";
    private final String PARSE_TASK_BOLT = "parseTaskBolt";
    private final String RETRIEVE_FILE_BOLT = "retrieveFileBolt";
    private final String IC_BOLT = "icBolt";
    private final String GRANT_PERMISSIONS_TO_FILE_BOLT = "grantPermissionsToFileBolt";
    private final String REMOVE_PERMISSIONS_TO_FILE_BOLT = "removePermissionsToFileBolt";
    private final String END_BOLT = "EndBolt";
    private FileServiceClient fileServiceClient;
    private ImageConverterServiceImpl imageConverterService;
    private RecordServiceClient recordServiceClient;


    public ICSTopologyTest() {

    }


    @Before
    public void setUp() throws Exception {
        dirtyMockZookeeperKS();
        dirtyMockRecordSC();
        dirtyMockFileSC();
        dirtyMockImageCS();
    }


    @Test
    public void testBasicTopology() throws MCSException, IOException, ICSException, URISyntaxException {
        //given
        configureMocks();
        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"http://localhost:8080/mcs/records/BIOXZW32GHT4S4C4CV6MPMDZSVTQBGQ3FUCBIYN4LC2SRVCHMZ6A/representations/tiff/versions/7f0865b0-9441-11e5-9f0d-fa163e2dd531/files/052f8a6a-8f53-4aba-9158-d110c97e91a8\"]}," +
                "\"parameters\":" +
                "{\"MIME_TYPE\":\"image/tiff\"," +
                "\"OUTPUT_MIME_TYPE\":\"image/jp2\"," +
                "\"TASK_SUBMITTER_NAME\":\"user\"}," +
                "\"taskId\":-2249083708465856201," +
                "\"taskName\":\"taskName\"}";
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                // build the test topology
                ReadFileBolt retrieveFileBolt = new ReadFileBolt("", "", "");
                WriteRecordBolt writeRecordBolt = new WriteRecordBolt("", "", "");
                GrantPermissionsToFileBolt grantPermissionsToFileBolt = new GrantPermissionsToFileBolt("", "", "");
                RemovePermissionsToFileBolt removePermBolt = new RemovePermissionsToFileBolt("", "",
                        "");

                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout(SPOUT, new TestSpaut(), 1);

                builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt()).shuffleGrouping(SPOUT);
                builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(PARSE_TASK_BOLT);
                builder.setBolt(IC_BOLT, new IcBolt()).shuffleGrouping(RETRIEVE_FILE_BOLT);
                builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(IC_BOLT);
                builder.setBolt(GRANT_PERMISSIONS_TO_FILE_BOLT, grantPermissionsToFileBolt).shuffleGrouping(WRITE_RECORD_BOLT);
                builder.setBolt(REMOVE_PERMISSIONS_TO_FILE_BOLT, grantPermissionsToFileBolt).shuffleGrouping(GRANT_PERMISSIONS_TO_FILE_BOLT);
                builder.setBolt(END_BOLT,new EndBolt()).shuffleGrouping(REMOVE_PERMISSIONS_TO_FILE_BOLT);
                StormTopology topology = builder.createTopology();


                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(SPOUT, new Values(input));

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
                List<String> printOrder = Arrays.asList(SPOUT, PARSE_TASK_BOLT, RETRIEVE_FILE_BOLT, IC_BOLT, WRITE_RECORD_BOLT, GRANT_PERMISSIONS_TO_FILE_BOLT,REMOVE_PERMISSIONS_TO_FILE_BOLT,END_BOLT);
                for (String i : printOrder) {
                    List printIn = Testing.readTuples(result, i);
                    prettyPrintJSON(printIn, i);
                }


            }

        });
    }

    private void prettyPrintJSON(List printIn, String input) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(printIn);
        System.out.println("=============================" + input + "=============================\n" + json);
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
        recordServiceClient = Mockito.mock(RecordServiceClient.class);
        PowerMockito.whenNew(RecordServiceClient.class).withAnyArguments().thenReturn(recordServiceClient);

    }

    private void dirtyMockFileSC() throws Exception {
        fileServiceClient = Mockito.mock(FileServiceClient.class);
        PowerMockito.whenNew(FileServiceClient.class).withAnyArguments().thenReturn(fileServiceClient);

    }

    private void dirtyMockImageCS() throws Exception {
        imageConverterService = Mockito.mock(ImageConverterServiceImpl.class);
        PowerMockito.whenNew(ImageConverterServiceImpl.class).withAnyArguments().thenReturn(imageConverterService);

    }

    private void configureMocks() throws MCSException, IOException, ICSException, URISyntaxException {
        when(fileServiceClient.getFile(anyString())).thenReturn(new ByteArrayInputStream("testContent".getBytes()));
        doNothing().when(imageConverterService).convertFile(any(StormTaskTuple.class));
        Representation representation = new Representation();
        representation.setDataProvider("TestDataProvider");
        when(recordServiceClient.getRepresentation(anyString(), anyString(), anyString())).thenReturn(representation);
        String cloudId = "cloudID";
        String representationName = "RepresentationName";
        String version = "version";
        String versionString = "http://localhost:8080/mcs/records/" + cloudId + "/representations/" + representationName + "/versions/" + version;
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(versionString));
        String file = "file";
        when(fileServiceClient.uploadFile(anyString(), any(InputStream.class), anyString())).thenReturn(new URI(versionString + "/files/" + file));
        when(recordServiceClient.persistRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(versionString));
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        doNothing().when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
    }
}