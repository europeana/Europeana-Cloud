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
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.*;
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
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, ReadDatasetsBolt.class, ReadRepresentationBolt.class, ReadDataSetBolt.class, IcBolt.class, WriteRecordBolt.class,  NotificationBolt.class})

@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ICTopologyTest extends ICTestMocksHelper implements TestConstantsHelper {

    private final String datasetStream = "DATASET_URLS";
    private final String fileStream = "FILE_URLS";
    Map<String, String> routingRules;


    public ICTopologyTest() {

    }


    @Before
    public void setUp() throws Exception {
        mockZookeeperKS();
        mockRecordSC();
        mockFileSC();
        mockImageCS();
        mockDPSDAO();
        mockDatSetClient();
        routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.FILE_URLS, datasetStream);
        routingRules.put(PluginParameterKeys.DATASET_URLS, fileStream);
    }


    @Test
    public void testBasicTopology() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        configureMocks();
        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"" + SOURCE_VERSION_URL + "\"]}," +
                "\"parameters\":" +
                "{\"MIME_TYPE\":\"image/tiff\"," +
                "\"OUTPUT_MIME_TYPE\":\"image/jp2\"," +
                "\"AUTHORIZATION_HEADER\":\"AUTHORIZATION_HEADER\"}," +
                "\"taskId\":1," +
                "\"taskName\":\"taskName\"}";
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                StormTopology topology = buildTopology();
                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(SPOUT, new Values(input));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                String expectedTuple = "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\": \"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"resource\":\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\",\"state\":\"SUCCESS\",\"additionalInfo\":\"\"}]]";

                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuple);
            }
        });
    }


    @Test
    public void testTopologyWithDataSetsAsDataEntry() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        configureMocks();
        final String input = "{\"inputData\":" +
                "{\"DATASET_URLS\":" +
                "[\"" + SOURCE_DATASET_URL + "\"]}," +
                "\"parameters\":" +
                "{\"MIME_TYPE\":\"image/tiff\"," +
                "\"OUTPUT_MIME_TYPE\":\"image/jp2\"," +
                "\"AUTHORIZATION_HEADER\":\"AUTHORIZATION_HEADER\"}," +
                "\"taskId\":1," +
                "\"taskName\":\"taskName\"}";
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                // build the test topology
                StormTopology topology = buildTopology();
                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(SPOUT, new Values(input));

                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);

                String expectedTuple = "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\": \"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"resource\":\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\",\"state\":\"SUCCESS\",\"additionalInfo\":\"\"}]]";

                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuple);

            }
        });
    }


    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private void configureMocks() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {

        String fileUrl = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";

        List<File> files = new ArrayList<>();
        files.add(new File("sourceFileName","text/plain", "md5","1",5,new URI(fileUrl)));
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER,files,false,new Date());

        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetClient).useAuthorizationHeader(anyString());
        when(fileServiceClient.getFile(anyString())).thenReturn(new ByteArrayInputStream("testContent".getBytes()));

        List<Representation> representationList = new ArrayList<>();
        representationList.add(representation);


        List<File> files2 = new ArrayList<>();
        files2.add(new File("sourceFileName","text/plain", "md5","1",5,new URI(fileUrl)));
        Representation representation2 = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2, new URI(SOURCE_VERSION_URL2), new URI(SOURCE_VERSION_URL2), DATA_PROVIDER,files2,false,new Date());
        List<Representation> representationList2 = new ArrayList<>();
        representationList2.add(representation2);

        when(dataSetClient.getDataSetRepresentations("testDataProvider", "dataSet")).thenReturn(representationList);
        when(dataSetClient.getDataSetRepresentations("testDataProvider", "dataSet2")).thenReturn(representationList2);

        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL2));

        doNothing().when(imageConverterService).convertFile(any(StormTaskTuple.class));

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(representation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2)).thenReturn(representation2);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));
        when(fileServiceClient.uploadFile(anyString(), any(InputStream.class), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        when(recordServiceClient.persistRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));

    }

    private StormTopology buildTopology() {
        // build the test topology
        ReadFileBolt retrieveFileBolt = new ReadFileBolt("");
        ReadDatasetsBolt readDatasetsBolt = new ReadDatasetsBolt();
        ReadDataSetBolt readDataSetBolt = new ReadDataSetBolt("");
        ReadRepresentationBolt readRepresentationBolt = new ReadRepresentationBolt("");
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt("");
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT, new TestSpout(), 1);
        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules)).shuffleGrouping(SPOUT);


        builder.setBolt(READ_DATASETS_BOLT, readDatasetsBolt).shuffleGrouping(PARSE_TASK_BOLT, datasetStream);
        builder.setBolt(READ_DATASET_BOLT, readDataSetBolt).shuffleGrouping(READ_DATASETS_BOLT);
        builder.setBolt(READ_REPRESENTATION_BOLT, readRepresentationBolt).shuffleGrouping(READ_DATASET_BOLT);

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(PARSE_TASK_BOLT, fileStream)
                .shuffleGrouping(READ_REPRESENTATION_BOLT);

        builder.setBolt(IC_BOLT, new IcBolt()).shuffleGrouping(RETRIEVE_FILE_BOLT);
        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(IC_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
        builder.setBolt(NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASETS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_REPRESENTATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(IC_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));

        StormTopology topology = builder.createTopology();
        return topology;
    }

    private CompleteTopologyParam prepareCompleteTopologyParam(MockedSources mockedSources) {
        // prepare the config
        Config conf = new Config();
        conf.setNumWorkers(NUM_WORKERS);
        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedSources);
        completeTopologyParam.setStormConf(conf);
        return completeTopologyParam;

    }

    private void assertResultedTuple(ILocalCluster cluster, StormTopology topology, CompleteTopologyParam completeTopologyParam, String expectedTuple) throws JSONException {

        //when
        Map result = Testing.completeTopology(cluster, topology,
                completeTopologyParam);
        //then
        printDefaultStreamTuples(result);

        String actual = parse(Testing.readTuples(result, WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME));
        assertEquals(expectedTuple, actual, true);
    }
}