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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.bolts.TestInspectionBolt;
import eu.europeana.cloud.bolts.TestSpout;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.helpers.TestConstantsHelper;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.GrantPermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RemovePermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.tika.mime.MimeTypeException;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, IcBolt.class, WriteRecordBolt.class, GrantPermissionsToFileBolt.class, RemovePermissionsToFileBolt.class, EndBolt.class, NotificationBolt.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ICTopologyTest extends ICTestMocksHelper implements TestConstantsHelper {


    public ICTopologyTest() {

    }


    @Before
    public void setUp() throws Exception {
        mockZookeeperKS();
        mockRecordSC();
        mockFileSC();
        mockImageCS();
        mockDPSDAO();
    }


    @Test
    public void testBasicTopology() throws MCSException,MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        configureMocks();
        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"" + SOURCE_VERSION_URL + "\"]}," +
                "\"parameters\":" +
                "{\"MIME_TYPE\":\"image/tiff\"," +
                "\"OUTPUT_MIME_TYPE\":\"image/jp2\"," +
                "\"TASK_SUBMITTER_NAME\":\"user\"}," +
                "\"taskId\":1," +
                "\"taskName\":\"taskName\"}";
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                // build the test topology
                ReadFileBolt retrieveFileBolt = new ReadFileBolt("", "", "");
                WriteRecordBolt writeRecordBolt = new WriteRecordBolt("", "", "");
                GrantPermissionsToFileBolt grantPermissionsToFileBolt = new GrantPermissionsToFileBolt("", "", "");
                RemovePermissionsToFileBolt removePermBolt = new RemovePermissionsToFileBolt("", "", "");
                NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
                TestInspectionBolt endTest = new TestInspectionBolt();

                TopologyBuilder builder = new TopologyBuilder();

                builder.setSpout(SPOUT, new TestSpout(), 1);
                builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt()).shuffleGrouping(SPOUT);
                builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(PARSE_TASK_BOLT);
                builder.setBolt(IC_BOLT, new IcBolt()).shuffleGrouping(RETRIEVE_FILE_BOLT);
                builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(IC_BOLT);
                builder.setBolt(GRANT_PERMISSIONS_TO_FILE_BOLT, grantPermissionsToFileBolt).shuffleGrouping(WRITE_RECORD_BOLT);
                builder.setBolt(REMOVE_PERMISSIONS_TO_FILE_BOLT, removePermBolt).shuffleGrouping(GRANT_PERMISSIONS_TO_FILE_BOLT);
                builder.setBolt(END_BOLT, new EndBolt()).shuffleGrouping(REMOVE_PERMISSIONS_TO_FILE_BOLT);
                builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(END_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
                builder.setBolt(NOTIFICATION_BOLT, notificationBolt)
                        .fieldsGrouping(PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(IC_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(GRANT_PERMISSIONS_TO_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(REMOVE_PERMISSIONS_TO_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                        .fieldsGrouping(END_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));


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
                printDefaultStreamTuples(result);

                String actual = parse(Testing.readTuples(result, END_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME));
                String expectedTuple = "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\": \"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"resource\":\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\",\"state\":\"SUCCESS\",\"additionalInfo\":\"\"}]]";

                assertEquals(expectedTuple, actual, true);
            }
        });
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }


    private void configureMocks() throws MCSException,MimeTypeException, IOException, ICSException, URISyntaxException {
        when(fileServiceClient.getFile(anyString())).thenReturn(new ByteArrayInputStream("testContent".getBytes()));
        doNothing().when(imageConverterService).convertFile(any(StormTaskTuple.class));
        Representation representation = new Representation();
        representation.setDataProvider("TestDataProvider");
        when(recordServiceClient.getRepresentation(anyString(), anyString(), anyString())).thenReturn(representation);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));
        when(fileServiceClient.uploadFile(anyString(), any(InputStream.class), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        when(recordServiceClient.persistRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
        doNothing().when(recordServiceClient).revokePermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(Permission.class));
    }
}