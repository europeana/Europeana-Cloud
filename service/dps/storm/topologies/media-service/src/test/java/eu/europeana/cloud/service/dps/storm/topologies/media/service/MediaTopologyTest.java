package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.model.*;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
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
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 12/17/2018.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, MediaTopology.class, EDMObjectProcessorBolt.class, ParseFileForMediaBolt.class, EDMEnrichmentBolt.class, RevisionWriterBolt.class, NotificationBolt.class, CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class, CassandraTaskErrorsDAO.class, CassandraNodeStatisticsDAO.class, WriteRecordBolt.class, TaskStatusChecker.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "org.apache.logging.log4j.*", "javax.xml.*", "org.xml.sax.*", "org.w3c.dom.*", "javax.activation.*"})

public class MediaTopologyTest extends TopologyTestHelper {
    private static StormTopology topology;
    private static final String AUTHORIZATION = "Authorization";
    @Mock
    private AmazonClient amazonClient;

    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_FILE_BOLT, TopologyHelper.RESOURCE_PROCESSING_BOLT, TopologyHelper.EDM_ENRICHMENT_BOLT, TopologyHelper.WRITE_RECORD_BOLT, TopologyHelper.REVISION_WRITER_BOLT, TopologyHelper.WRITE_TO_DATA_SET_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);

    private void mockMediaExtractor() throws Exception {
        MediaExtractor mediaExtractor = mock(MediaExtractor.class);
        MediaProcessorFactory mediaProcessorFactory = mock(MediaProcessorFactory.class);
        PowerMockito.whenNew(MediaProcessorFactory.class).withAnyArguments().thenReturn(mediaProcessorFactory);
        when(mediaProcessorFactory.createMediaExtractor()).thenReturn(mediaExtractor);
        String thumbnailName = "thumbnailName";
        String resourceName = "resourceName";


        List<Thumbnail> thumbnailList = new ArrayList<>();
        Thumbnail thumbnail =mock(Thumbnail.class);
        when(thumbnail.getTargetName()).thenReturn(thumbnailName);
        when(thumbnail.getContentSize()).thenReturn(1l);
        thumbnailList.add(thumbnail);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class))).thenReturn(resourceExtractionResult);
        when(amazonClient.putObject(anyString(), anyString(), any(InputStream.class), isNull(ObjectMetadata.class))).thenReturn(new PutObjectResult());
    }

    @Before
    public final void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        buildTopology();
        mockRecordSC();
        mockFileSC();
        mockCassandraInteraction();
        mockDatSetClient();
        mockRevisionServiceClient();
        configureMocks();
        mockMediaExtractor();


    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,\"NOTIFICATION\",{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
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
        List actualTuples = Testing.readTuples(result, TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
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

    private void buildTopology() {
        // build the test topology
        EDMObjectProcessorBolt edmObjectProcessorBolt = new EDMObjectProcessorBolt(MCS_URL, amazonClient);
        ResourceProcessingBolt resourceProcessingBolt = new ResourceProcessingBolt(amazonClient);
        ParseFileForMediaBolt parseFileBolt = new ParseFileForMediaBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(MCS_URL);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(MCS_URL);
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(MCS_URL);
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);
        builder.setBolt(TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT, edmObjectProcessorBolt).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.PARSE_FILE_BOLT, parseFileBolt).shuffleGrouping(TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT);
        builder.setBolt(TopologyHelper.RESOURCE_PROCESSING_BOLT, resourceProcessingBolt).shuffleGrouping(TopologyHelper.PARSE_FILE_BOLT);
        builder.setBolt(TopologyHelper.EDM_ENRICHMENT_BOLT, new EDMEnrichmentBolt(MCS_URL)).fieldsGrouping(TopologyHelper.PARSE_FILE_BOLT, new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY));
        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(TopologyHelper.EDM_ENRICHMENT_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt).shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.EDM_OBJECT_PROCESSOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.PARSE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.RESOURCE_PROCESSING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.EDM_ENRICHMENT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));
        topology = builder.createTopology();

    }

}