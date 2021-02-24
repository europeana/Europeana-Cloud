package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForLinkCheckBolt;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForMediaBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.mediaprocessing.LinkChecker;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.json.JSONException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 2/19/2019.
 */


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, LinkCheckTopology.class, LinkCheckBolt.class, ParseFileForMediaBolt.class, NotificationBolt.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, ReadFileBolt.class, TaskStatusChecker.class, TaskStatusUpdater.class, ProcessedRecordsDAO.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "org.apache.logging.log4j.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "com.sun.org.apache.xerces.*", "javax.xml.parsers.*"})

public class LinkCheckTopologyTest extends TopologyTestHelper {

    private static StormTopology topology;
    private static final String AUTHORIZATION = "Authorization";

    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_FILE_BOLT, TopologyHelper.LINK_CHECK_BOLT, TEST_END_BOLT);

    @BeforeClass
    public static void init() {
        buildTopology();
    }


    private void mockLinkChecking() throws Exception {
        LinkChecker linkChecker = mock(LinkChecker.class);
        MediaProcessorFactory mediaProcessorFactory = mock(MediaProcessorFactory.class);
        PowerMockito.whenNew(MediaProcessorFactory.class).withAnyArguments().thenReturn(mediaProcessorFactory);
        when(mediaProcessorFactory.createLinkChecker()).thenReturn(linkChecker);
        doNothing().when(linkChecker).performLinkChecking(anyString());
    }

    @Before
    public final void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mockRecordSC();
        mockFileSC();
        mockCassandraInteraction();
        mockDatSetClient();
        mockRevisionServiceClient();
        configureMocks();
        mockLinkChecking();


    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"START_TIME\":1,\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"\",\"resultResource\":\"\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
    }

    @Test
    public final void shouldTestSuccessfulExecution() throws MCSException, IOException, URISyntaxException {
        //given
        prepareForTask();

        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(1);
        Map<String, String> taskParameters = new HashMap<>();
        taskParameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        taskParameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.AUTHORIZATION_HEADER);
        taskParameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE_VERSION_URL);
        dpsTask.setParameters(taskParameters);
        dpsTask.setInputData(null);
        dpsTask.setOutputRevision(new Revision());
        dpsTask.setHarvestingDetails(new OAIPMHHarvestingDetails());
        dpsTask.setTaskName("Task_Name");

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                dpsTask.getTaskId(),
                dpsTask.getTaskName(),
                null, null, taskParameters, dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());
        stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");

        assertTopology(stormTaskTuple);

    }


    private final void prepareForTask() throws URISyntaxException, IOException, MCSException {
        List<File> files = new ArrayList<>(1);
        List<Revision> revisions = new ArrayList<>(1);
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, files, revisions, false, new Date());
        when(fileServiceClient.getFile(SOURCE_VERSION_URL, AUTHORIZATION, PluginParameterKeys.AUTHORIZATION_HEADER)).thenReturn(new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/files/one-resource.xml")))).thenReturn(new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/files/one-resource.xml"))));
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, AUTHORIZATION, PluginParameterKeys.AUTHORIZATION_HEADER)).thenReturn(representation);
    }


    private void configureMocks() throws MCSException, IOException, URISyntaxException {
        when(recordServiceClient.createRepresentation(anyString(), anyString(), anyString(), any(InputStream.class), anyString(), anyString(), anyString(), anyString())).thenReturn(new URI(RESULT_FILE_URL));
        doNothing().when(dataSetClient).assignRepresentationToDataSet(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString());
        when(revisionServiceClient.addRevision(anyString(), anyString(), anyString(), isA(Revision.class), anyString(), anyString())).thenReturn(new URI(REVISION_URL));
    }


    private void assertResultedTuple(ILocalCluster cluster, StormTopology topology, CompleteTopologyParam completeTopologyParam, List<String> expectedTuples) throws JSONException {
        Map result = Testing.completeTopology(cluster, topology,
                completeTopologyParam);
        //then
        printDefaultStreamTuples(result);
        List actualTuples = Testing.readTuples(result, TopologyHelper.LINK_CHECK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
        for (int i = 0; i < expectedTuples.size(); i++) {
            String actual = parse(selectSingle(actualTuples, i));
            String expected = expectedTuples.get(i);
            assertEquals(expected, actual);
        }
    }

    private List selectSingle(List actualTuples, int index) {
        return Arrays.asList(actualTuples.get(index));
    }

    private void printDefaultStreamTuples(Map result) {
        for (String boltResult : PRINT_ORDER) {
            prettyPrintJSON(Testing.readTuples(result, boltResult), boltResult);
        }
    }

    private static void buildTopology() {
        // build the test topology
        ParseFileForLinkCheckBolt parseFileBolt = new ParseFileForLinkCheckBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.PARSE_FILE_BOLT, parseFileBolt).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.LINK_CHECK_BOLT, new LinkCheckBolt()).shuffleGrouping(TopologyHelper.PARSE_FILE_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.LINK_CHECK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.PARSE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.LINK_CHECK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));
        topology = builder.createTopology();

    }

}

