package eu.europeana.cloud;


import eu.europeana.cloud.bolts.TestInspectionBolt;
import eu.europeana.cloud.bolts.TestSpout;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TestConstantsHelper;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.tika.mime.MimeTypeException;
import org.json.JSONException;
import org.junit.Before;
import org.junit.BeforeClass;
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
@PrepareForTest({ReadFileBolt.class, ReadDatasetsBolt.class, ReadRepresentationBolt.class, ReadDatasetBolt.class, IcBolt.class, WriteRecordBolt.class, AddResultToDataSetBolt.class, NotificationBolt.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*"})
public class ICTopologyTest extends ICTestMocksHelper implements TestConstantsHelper {

    private static final String DATASET_STREAM = "DATASET_URLS";
    private static final String FILE_STREAM = "FILE_URLS";
    private static final String TASK_PARAMETERS = "\"parameters\":" +
            "{\"MIME_TYPE\":\"image/tiff\"," +
            "\"OUTPUT_MIME_TYPE\":\"image/jp2\"," +
            "\"OUTPUT_DATA_SETS\":\"http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet\"," +
            "\"AUTHORIZATION_HEADER\":\"AUTHORIZATION_HEADER\"}," +
            "\"taskId\":1," +
            "\"taskName\":\"taskName\"}";
    private static final String FILE_CONTENTS = "    [\n" +
            "      116,\n" +
            "      101,\n" +
            "      115,\n" +
            "      116,\n" +
            "      67,\n" +
            "      111,\n" +
            "      110,\n" +
            "      116,\n" +
            "      101,\n" +
            "      110,\n" +
            "      116\n" +
            "    ],\n" +
            "    {\n" +
            "      \"MIME_TYPE\": \"image/tiff\",\n" +
            "      \"OUTPUT_MIME_TYPE\": \"image/jp2\",\n" +
            "      \"AUTHORIZATION_HEADER\": \"AUTHORIZATION_HEADER\",\n" +
            "    }\n" +
            "  ]\n" +
            "]";
    private static Map<String, String> routingRules;
    private static StormTopology topology;


    public ICTopologyTest() {

    }


    @BeforeClass
    public static void buildToplogy() {
        routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.FILE_URLS, DATASET_STREAM);
        routingRules.put(PluginParameterKeys.DATASET_URLS, FILE_STREAM);
        buildTopology();

    }

    @Before
    public final void setUp() throws Exception {
        mockZookeeperKS();
        mockRecordSC();
        mockFileSC();
        mockImageCS();
        mockDPSDAO();
        mockDatSetClient();
        configureMocks();
        mockRepresentationIterator();
    }

    private void assertTopology(final String input) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, new Values(input));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                String expectedTuple = "[[1,\"NOTIFICATION\",{\"info_text\":\"\",\"resultResource\": \"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"resource\":\"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\",\"state\":\"SUCCESS\",\"additionalInfo\":\"\"}]]";
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuple);
            }
        });
    }

    @Test
    public final void testBasicTopology() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        prepareForSingleDataset();

        final String input = "{\"inputData\":" +
                "{\"FILE_URLS\":" +
                "[\"" + SOURCE_VERSION_URL + "\"]}," +
                TASK_PARAMETERS;
        assertTopology(input);

    }


    @Test
    public final void testTopologyWithSingleDataSetAsDataEntry() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        prepareForSingleDataset();
        final String input = "{\"inputData\":" +
                "{\"DATASET_URLS\":" +
                "[\"" + SOURCE_DATASET_URL + "\"]}," +
                TASK_PARAMETERS;

        assertTopology(input);
    }


    @Test
    public final void testTopologyWithMultipleDataSetsAsDataEntry() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        //given
        prepareForMultipleDatasets();
        final String inputTuple = "{\"inputData\":" +
                "{\"DATASET_URLS\":" +
                "[\"" + SOURCE_DATASET_URL + "\",\"" + SOURCE_DATASET_URL2 + "\"]}," +
                TASK_PARAMETERS;

        final List<String> expectedTuples = Arrays.asList("[\n" +
                        "  [\n" +
                        "    1,\n" +
                        "    \"taskName\",\n" +
                        "    \"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName\",\n" +
                        FILE_CONTENTS,
                " [ [\n" +
                        "    1,\n" +
                        "    \"taskName\",\n" +
                        "    \"http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion2/files/sourceFileName\",\n" +
                        FILE_CONTENTS);
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, new Values(inputTuple));
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);

            }
        });
    }


    private List selectSingle(List actualTuples, int index) {
        return Arrays.asList(actualTuples.get(index));
    }

    private void assertResultedTuple(ILocalCluster cluster, StormTopology topology, CompleteTopologyParam completeTopologyParam, List<String> expectedTuples) throws JSONException {
        Map result = Testing.completeTopology(cluster, topology,
                completeTopologyParam);
        //then
        printDefaultStreamTuples(result);

        List actualTuples = Testing.readTuples(result, TopologyHelper.IC_BOLT);
        for (int i = 0; i < expectedTuples.size(); i++) {
            String actual = parse(selectSingle(actualTuples, i));
            String expected = expectedTuples.get(i);
            assertEquals(expected, actual, false);
        }
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private void configureMocks() throws MCSException, MimeTypeException, IOException, ICSException, URISyntaxException {
        doNothing().when(fileServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetClient).useAuthorizationHeader(anyString());
        doNothing().when(imageConverterService).convertFile(any(StormTaskTuple.class));
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
    }


    public final void prepareForMultipleDatasets() throws URISyntaxException, IOException, MCSException {
        String fileUrl1 = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
        String fileUrl2 = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion2/files/sourceFileName";
        List<File> firstFilesList = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        firstFilesList.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(fileUrl1)));

        Representation firstRepresentation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, firstFilesList, revisions, false, new Date());
        List<File> secondFilesList = new ArrayList<>();
        secondFilesList.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(fileUrl2)));
        Representation secondRepresentation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2, new URI(SOURCE_VERSION_URL2), new URI(SOURCE_VERSION_URL2), DATA_PROVIDER, secondFilesList, revisions, false, new Date());
        when(dataSetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, true, false);
        when(representationIterator.next()).thenReturn(firstRepresentation).thenReturn(secondRepresentation);
        when(fileServiceClient.getFile(fileUrl1)).thenReturn(new ByteArrayInputStream("testContent".getBytes()));
        when(fileServiceClient.getFile(fileUrl2)).thenReturn(new ByteArrayInputStream("testContent".getBytes()));
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2)).thenReturn(secondRepresentation);
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION + 2, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL2));
    }


    public final void prepareForSingleDataset() throws URISyntaxException, IOException, MCSException {
        String fileUrl = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";
        List<File> files = new ArrayList<>();
        List<Revision> revisions = new ArrayList<>();
        files.add(new File("sourceFileName", "text/plain", "md5", "1", 5, new URI(fileUrl)));

        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, files, revisions, false, new Date());

        when(dataSetClient.getRepresentationIterator(anyString(), anyString())).thenReturn(representationIterator);
        when(representationIterator.hasNext()).thenReturn(true, false);
        when(representationIterator.next()).thenReturn(representation);
        when(fileServiceClient.getFileUri(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE + FILE)).thenReturn(new URI(SOURCE_VERSION_URL));
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(representation);
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));
        when(fileServiceClient.uploadFile(anyString(), any(InputStream.class), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        when(recordServiceClient.persistRepresentation(anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_VERSION_URL));


    }


    private static void buildTopology() {
        // build the test topology
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(MCS_URL);
        ReadDatasetsBolt readDatasetsBolt = new ReadDatasetsBolt();
        ReadDatasetBolt readDataSetBolt = new ReadDatasetBolt(MCS_URL);
        ReadRepresentationBolt readRepresentationBolt = new ReadRepresentationBolt(MCS_URL);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(MCS_URL);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules)).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.READ_DATASETS_BOLT, readDatasetsBolt).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, DATASET_STREAM);
        builder.setBolt(TopologyHelper.READ_DATASET_BOLT, readDataSetBolt).shuffleGrouping(TopologyHelper.READ_DATASETS_BOLT);
        builder.setBolt(TopologyHelper.READ_REPRESENTATION_BOLT, readRepresentationBolt).shuffleGrouping(TopologyHelper.READ_DATASET_BOLT);
        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, FILE_STREAM)
                .shuffleGrouping(TopologyHelper.READ_REPRESENTATION_BOLT);
        builder.setBolt(TopologyHelper.IC_BOLT, new IcBolt()).shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(TopologyHelper.IC_BOLT);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt).shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASETS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_REPRESENTATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.IC_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));
        topology = builder.createTopology();


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

        String actual = parse(Testing.readTuples(result, TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME));
        assertEquals(expectedTuple, actual, true);
    }
}
