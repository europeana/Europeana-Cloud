package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.service.ValidationStatisticsServiceImpl;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordData;
import eu.europeana.cloud.service.dps.storm.tuple.common.ProcessingData;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskData;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class StatisticsBoltTest extends CassandraTestBase {

    private static final long TASK_ID = 1;

    private static final String TASK_NAME = "task1";

    private ValidationStatisticsServiceImpl statisticsService;

    @Mock(name = "outputCollector")
    private OutputCollector collector;

    @InjectMocks
    private StatisticsBolt statisticsBolt = new StatisticsBolt(new CassandraProperties(),
            HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");

    @BeforeEach
    void setUp() {
        statisticsBolt.prepare();
        statisticsService = Mockito.spy(ValidationStatisticsServiceImpl.getInstance(
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, "",
                        "")));
    }

    @Test
    void testCountStatisticsSuccessfully() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL, fileData, true),
                new ProcessingData());
        tuple.setParameters(prepareStormTaskTupleParameters());
        List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

        //when
        statisticsBolt.execute(anchorTuple, tuple);

        //then
        assertSuccess(1);
        assertDataStoring(generated);
    }

    @Test
    void testAggregatedCountStatisticsSuccessfully() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        Tuple anchorTuple2 = mock(TupleImpl.class);
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL, fileData, true),
                new ProcessingData());
        tuple.setParameters(prepareStormTaskTupleParameters());
        List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

        byte[] fileData2 = Files.readAllBytes(Paths.get("src/test/resources/example2.xml"));

        CommonTaskTuple tuple2 = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL_CLOUD_ID2, fileData2, true),
                new ProcessingData());
        tuple2.setParameters(prepareStormTaskTupleParameters());
        List<NodeStatistics> generated2 = new RecordStatisticsGenerator(new String(fileData2)).getStatistics();

        //when
        statisticsBolt.execute(anchorTuple, tuple);
        statisticsBolt.execute(anchorTuple2, tuple2);

        //then
        assertSuccess(2);
        assertDataStoring(generated, generated2);
    }

    @Test
    void shouldNotGenerateStatisticsBecauseOfLackOfTheParameter() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL, fileData, true),
                new ProcessingData());
        tuple.setParameters(prepareStormTaskTupleParameters());

        //when
        statisticsBolt.execute(anchorTuple, tuple);

        //then
        assertSuccess(1);
        Mockito.verifyNoInteractions(statisticsService);
    }

    @Test
    void shouldNotGenerateStatisticsBecauseOfWrongParameter() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL, fileData, true),
                new ProcessingData());
        tuple.setParameters(prepareStormTaskTupleParameters());

        //when
        statisticsBolt.execute(anchorTuple, tuple);

        //then
        Mockito.verifyNoInteractions(statisticsService);
    }

    @Test
    void testCountStatisticsFailed() throws Exception {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        fileData[0] = 'X'; // will cause SAXException
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(TASK_ID, TASK_NAME),
                new RecordData(SOURCE_VERSION_URL, fileData, true),
                new ProcessingData());
        tuple.setParameters(prepareStormTaskTupleParameters());
        //when
        statisticsBolt.execute(anchorTuple, tuple);
        //then
        assertFailure();
    }

    private void assertDataStoring(List<NodeStatistics> generated, List<NodeStatistics> generated2) {
        List<NodeStatistics> statistics = statisticsService.getNodeStatistics(TASK_ID);

        assertEquals(statistics.size(), generated.size());
        for (NodeStatistics stats : statistics) {
            NodeStatistics nodeGenerated = findNode(stats, generated);
            long nodeGeneratedOccurrence = nodeGenerated != null ? nodeGenerated.getOccurrence() : 0;
            NodeStatistics nodeGenerated2 = findNode(stats, generated2);
            long nodeGeneratedOccurrence2 = nodeGenerated2 != null ? nodeGenerated2.getOccurrence() : 0;
            assertEquals(stats.getOccurrence(), nodeGeneratedOccurrence + nodeGeneratedOccurrence2);
            for (AttributeStatistics attrStats : stats.getAttributesStatistics()) {
                AttributeStatistics nodeAttrStats = findNodeAttributes(attrStats, nodeGenerated.getAttributesStatistics());
                long nodeAttrsOccurrence = nodeAttrStats != null ? nodeAttrStats.getOccurrence() : 0;
                AttributeStatistics nodeAttrStats2 = findNodeAttributes(attrStats, nodeGenerated2.getAttributesStatistics());
                long nodeAttrsOccurrence2 = nodeAttrStats2 != null ? nodeAttrStats2.getOccurrence() : 0;
                assertEquals(attrStats.getOccurrence(), nodeAttrsOccurrence + nodeAttrsOccurrence2);
            }
        }
    }

    private AttributeStatistics findNodeAttributes(AttributeStatistics attr, Set<AttributeStatistics> statistics) {
        for (AttributeStatistics attributeStatistics : statistics) {
            if (attributeStatistics.getName().equals(attr.getName()) &&
                    attributeStatistics.getValue().equals(attr.getValue())) {
                return attributeStatistics;
            }
        }
        return null;
    }

    private NodeStatistics findNode(NodeStatistics node, List<NodeStatistics> statistics) {
        for (NodeStatistics nodeStatistics : statistics) {
            if (nodeStatistics.getParentXpath().equals(node.getParentXpath()) &&
                    nodeStatistics.getXpath().equals(node.getXpath()) &&
                    nodeStatistics.getValue().equals(node.getValue())) {
                return nodeStatistics;
            }
        }
        return null;
    }

    private void assertDataStoring(List<NodeStatistics> generated) {
        List<NodeStatistics> statistics = statisticsService.getNodeStatistics(TASK_ID);
        assertEquals(statistics.size(), generated.size());
        assertTrue(statistics.containsAll(generated));
    }

    private void assertSuccess(int times) {
        Mockito.verify(collector, Mockito.times(times)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(0))
                .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private void assertFailure() {
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(1))
                .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        parameters.put(PluginParameterKeys.GENERATE_STATS, "true");
        return parameters;
    }

    private HashMap<String, String> prepareStormTaskTupleParametersWithoutStatsParam() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }

    private HashMap<String, String> prepareStormTaskTupleParametersWithWrongStatsParam() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        parameters.put(PluginParameterKeys.GENERATE_STATS, "trues");
        return parameters;
    }
}
