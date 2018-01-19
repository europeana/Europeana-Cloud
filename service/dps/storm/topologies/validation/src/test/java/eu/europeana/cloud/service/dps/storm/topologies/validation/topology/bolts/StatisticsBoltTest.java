package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.statistics.RecordStatisticsGenerator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StatisticsBolt.class)
@PowerMockIgnore("javax.management.*")
public class StatisticsBoltTest extends CassandraTestBase {
    private static final long TASK_ID = 1;

    private static final String TASK_NAME = "task1";

    private OutputCollector collector;

    private StatisticsBolt statisticsBolt;

    private CassandraNodeStatisticsDAO statisticsDAO;

    @Before
    public void setUp() throws Exception {
        collector = Mockito.mock(OutputCollector.class);
        statisticsBolt = new StatisticsBolt(HOST, PORT, KEYSPACE, "", "");

        Map<String, Object> boltConfig = new HashMap<>();
        boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
        boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
        boltConfig.put(Config.TOPOLOGY_NAME, "");
        statisticsBolt.prepare(boltConfig, null, collector);
        statisticsDAO = CassandraNodeStatisticsDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));

    }

    @Test
    public void testCountStatisticsSuccessfull() throws Exception {
        //given
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, new HashMap<String, String>(), new Revision());
        List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

        //when
        statisticsBolt.execute(tuple);

        //then
        assertSuccess(1);
        assertDataStoring(generated);
    }

    @Test
    public void testAggregatedCountStatisticsSuccessfull() throws Exception {
        //given
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, new HashMap<String, String>(), new Revision());
        List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

        byte[] fileData2 = Files.readAllBytes(Paths.get("src/test/resources/example2.xml"));
        StormTaskTuple tuple2 = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData2, new HashMap<String, String>(), new Revision());
        List<NodeStatistics> generated2 = new RecordStatisticsGenerator(new String(fileData2)).getStatistics();

        //when
        statisticsBolt.execute(tuple);
        statisticsBolt.execute(tuple2);

        //then
        assertSuccess(2);
        assertDataStoring(generated, generated2);
    }

    private void assertDataStoring(List<NodeStatistics> generated, List<NodeStatistics> generated2) {
        List<NodeStatistics> statistics = statisticsDAO.getNodeStatistics(TASK_ID);

        Assert.assertEquals(statistics.size(), generated.size());
        for (NodeStatistics stats : statistics) {
            NodeStatistics nodeGenerated = findNode(stats, generated);
            long nodeGeneratedOccurrence = nodeGenerated != null ? nodeGenerated.getOccurrence() : 0;
            NodeStatistics nodeGenerated2 = findNode(stats, generated2);
            long nodeGeneratedOccurrence2 = nodeGenerated2 != null ? nodeGenerated2.getOccurrence() : 0;
            Assert.assertEquals(stats.getOccurrence(), nodeGeneratedOccurrence + nodeGeneratedOccurrence2);
            for (AttributeStatistics attrStats : stats.getAttributesStatistics()) {
                AttributeStatistics nodeAttrStats = findNodeAttributes(attrStats, nodeGenerated.getAttributesStatistics());
                long nodeAttrsOccurrence = nodeAttrStats != null ? nodeAttrStats.getOccurrence() : 0;
                AttributeStatistics nodeAttrStats2 = findNodeAttributes(attrStats, nodeGenerated2.getAttributesStatistics());
                long nodeAttrsOccurrence2 = nodeAttrStats2 != null ? nodeAttrStats2.getOccurrence() : 0;
                Assert.assertEquals(attrStats.getOccurrence(), nodeAttrsOccurrence + nodeAttrsOccurrence2);
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
        List<NodeStatistics> statistics = statisticsDAO.getNodeStatistics(TASK_ID);
        Assert.assertEquals(statistics.size(), generated.size());
        Assert.assertTrue(statistics.containsAll(generated));
    }

    @Test
    public void testCountStatisticsFailed() throws Exception {
        //given
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        fileData[0]='X'; // will cause SAXException
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, new HashMap<String, String>(), new Revision());
        //when
        statisticsBolt.execute(tuple);
        //then
        assertFailure();
    }

    private void assertSuccess(int times) {
        Mockito.verify(collector, Mockito.times(times)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private void assertFailure() {
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }
}
