package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CassandraNodeStatisticsDAOTest extends CassandraTestBase {
    private static final long TASK_ID = 1;

    private static final String ROOT_XPATH = "/root";

    private static final String NODE_1_XPATH = "/root/node1";

    private static final String NODE_2_XPATH = "/root/node2";

    private static final String NODE_VALUE_1 = "value1";

    private static final String NODE_VALUE_2 = "value2";

    private CassandraNodeStatisticsDAO nodeStatisticsDAO;

    @Before
    public void setUp() {
        nodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));
    }

    @Test
    public void testShouldProperlyStoreNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics();

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : toStore) {
            Assert.assertTrue(retrieved.contains(stats));
        }
    }

    private List<NodeStatistics> prepareNodeStatistics() {
        List<NodeStatistics> statistics = new ArrayList<>();
        statistics.add(new NodeStatistics(null, ROOT_XPATH, null, 1));
        statistics.add(new NodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_1, 1));
        statistics.add(new NodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_2, 1));
        statistics.add(new NodeStatistics(ROOT_XPATH, NODE_2_XPATH, NODE_VALUE_1, 1));
        return statistics;
    }
}
