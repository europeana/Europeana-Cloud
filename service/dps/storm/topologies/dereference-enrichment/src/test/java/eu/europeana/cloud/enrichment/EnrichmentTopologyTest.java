package eu.europeana.cloud.enrichment;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.enrichment.bolts.EnrichmentBolt;
import eu.europeana.cloud.enrichment.helper.EnrichmentMockHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.dao.CassandraDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
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
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;

import static eu.europeana.cloud.enrichment.bolts.EnrichmentBoltTest.DEREFERENCE_URL;
import static eu.europeana.cloud.enrichment.bolts.EnrichmentBoltTest.ENRICHMENT_URL;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static junit.framework.TestCase.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReadFileBolt.class, EnrichmentBolt.class, ValidationRevisionWriter.class, NotificationBolt.class,
        CassandraConnectionProviderSingleton.class, CassandraTaskInfoDAO.class, CassandraSubTaskInfoDAO.class,
        CassandraTaskErrorsDAO.class, CassandraNodeStatisticsDAO.class, WriteRecordBolt.class, ReadFileBolt.class,
        TaskStatusChecker.class, TaskStatusUpdater.class, TaskDiagnosticInfoDAO.class, CassandraDAO.class,
        HarvestedRecordsDAO.class, ProcessedRecordsDAO.class
        //,       org.apache.storm.util$exit_process_BANG_.class
})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "eu.europeana.cloud.test.CassandraTestInstance",
        "org.apache.logging.log4j.*","com.sun.org.apache.xerces.*","javax.xml.parsers.*","org.mockito.*",
        "javax.xml.*","org.springframework.util.*","com.ctc.wstx.*","com.sun.*"
//        ,"org.apache.*","clojure.*"
})
public class EnrichmentTopologyTest extends EnrichmentMockHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentTopologyTest.class);

    private static final String AUTHORIZATION = "Authorization";
    public static final byte[] FILE_DATA = "<a>B</b>".getBytes();
    private static StormTopology topology;
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.ENRICHMENT_BOLT, TopologyHelper.WRITE_RECORD_BOLT, TopologyHelper.REVISION_WRITER_BOLT, TopologyHelper.WRITE_TO_DATA_SET_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);



    @BeforeClass
    public static void buildToplogy() {

        buildTopology();
    }

    @Before
    public final void setUp() throws Exception {
        mockRecordSC();
        mockFileSC();
        mockCassandraInteraction();
        mockDatSetClient();
        mockRevisionServiceClient();
        configureMocks();
        mockEnrichmentService();
        mockRepresentationIterator();
    }

    private void assertTopology(final StormTaskTuple stormTaskTuple) {
        MkClusterParam mkClusterParam = prepareMKClusterParm();
        Testing.withLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws JSONException {
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(TopologyHelper.SPOUT, stormTaskTuple.toStormTuple());
                CompleteTopologyParam completeTopologyParam = prepareCompleteTopologyParam(mockedSources);
                final List<String> expectedTuples = Arrays.asList("[[1,{\"resource\":\"" + SOURCE_VERSION_URL + "\",\"info_text\":\"\",\"resultResource\":\"http://localhost:8080/mcs/records/resultCloudId/representations/resultRepresentationName/versions/resultVersion/files/FileName\",\"additionalInfo\":\"\",\"state\":\"SUCCESS\"}]]");
                assertResultedTuple(cluster, topology, completeTopologyParam, expectedTuples);
            }
        });
    }

    @Test
    public final void testTopology() throws MCSException, IOException, URISyntaxException {
        //given
        prepareForTask();

        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(1);
        Map<String, String> taskParameters = prepareTaskParameters();
        taskParameters.put(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, SOURCE_VERSION_URL);
        dpsTask.setParameters(taskParameters);
        dpsTask.setInputData(null);
        dpsTask.setOutputRevision(new Revision());
        dpsTask.setHarvestingDetails(new OAIPMHHarvestingDetails());
        dpsTask.setTaskName("Task_Name");


        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                dpsTask.getTaskId(),
                dpsTask.getTaskName(),
                SOURCE_VERSION_URL, FILE_DATA, taskParameters, dpsTask.getOutputRevision(), new OAIPMHHarvestingDetails());

        assertTopology(stormTaskTuple);

    }

    private Map<String, String> prepareTaskParameters() {
        Map<String, String> taskParameters = new HashMap<>();
        taskParameters.put(PluginParameterKeys.REPRESENTATION_NAME, SOURCE + REPRESENTATION_NAME);
        taskParameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, PluginParameterKeys.AUTHORIZATION_HEADER);
        taskParameters.put(PluginParameterKeys.DPS_TASK_INPUT_DATA, SOURCE_VERSION_URL);
        return taskParameters;
    }


    private final void prepareForTask() throws URISyntaxException, IOException, MCSException {
        List<File> files = new ArrayList<>(1);
        List<Revision> revisions = new ArrayList<>(1);
        Representation representation = new Representation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, new URI(SOURCE_VERSION_URL), new URI(SOURCE_VERSION_URL), DATA_PROVIDER, files, revisions, false, new Date());
        when(fileServiceClient.getFile(SOURCE_VERSION_URL, AUTHORIZATION, PluginParameterKeys.AUTHORIZATION_HEADER)).thenReturn(new ByteArrayInputStream("Valid_EDM_internal_content".getBytes(Charset.forName("UTF-8"))));
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
            assertEquals(expected, actual, false);
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
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(MCS_URL);
        NotificationBolt notificationBolt = new NotificationBolt("", 1, "", "", "");
        EnrichmentBolt enrichmentBolt = new EnrichmentBolt(DEREFERENCE_URL, ENRICHMENT_URL);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(MCS_URL);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(MCS_URL);
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(MCS_URL);
        TestInspectionBolt endTest = new TestInspectionBolt();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyHelper.SPOUT, new TestSpout(), 1);

        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt).shuffleGrouping(TopologyHelper.SPOUT);
        builder.setBolt(TopologyHelper.ENRICHMENT_BOLT, enrichmentBolt).shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);
        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt).shuffleGrouping(TopologyHelper.ENRICHMENT_BOLT);
        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt).shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt).shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);
        builder.setBolt(TEST_END_BOLT, endTest).shuffleGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME);

        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, notificationBolt)
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.ENRICHMENT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));
        topology = builder.createTopology();

    }
}

