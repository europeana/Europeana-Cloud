package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.Revision;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Ignore
    @Test
    public void testCountStatisticsSuccessfull() throws Exception {
        //given
        byte[] fileData = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, fileData, new HashMap<String, String>(), new Revision());
        List<NodeStatistics> generated = new RecordStatisticsGenerator(new String(fileData)).getStatistics();

        //when
        statisticsBolt.execute(tuple);
        //then
        assertSuccess();
        assertDataStoring(generated);
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

    private void assertSuccess() {
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private void assertFailure() {
        Mockito.verify(collector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(collector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }
}
