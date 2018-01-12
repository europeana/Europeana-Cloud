package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CassandraNodeStatisticsDAOTest extends CassandraTestBase {
    private static final long TASK_ID_1 = 1;

    private static final long TASK_ID_2 = 2;

    private static final long TASK_ID_3 = 3;

    private static final String ROOT_XPATH = "/root";

    private static final String NODE_1_XPATH = "/root/node1";

    private static final String NODE_2_XPATH = "/root/node2";

    private static final String NODE_3_XPATH = "/root/node3";

    private static final String NODE_4_XPATH = "/root/node3/node4";

    private static final String NODE_VALUE_1 = "value1";

    private static final String NODE_VALUE_2 = "value2";

    private static final String NODE_VALUE_3 = "value3";

    private static final String ATTRIBUTE_1_NAME = "attribute1";

    private static final String ATTRIBUTE_1_VALUE = "value1";

    private static final long OCCURRENCE = 1;

    private CassandraNodeStatisticsDAO nodeStatisticsDAO;

    @Before
    public void setUp() {
        nodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));
    }

    @Test
    public void testShouldProperlyStoreNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(false);

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID_1, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID_1);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : toStore) {
            Assert.assertTrue(retrieved.contains(stats));
        }
    }

    @Test
    public void testShouldProperlyUpdateNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(false);

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID_1, toStore);
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID_1, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID_1);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : toStore) {
            Assert.assertTrue(retrieved.contains(stats));
        }
        for (NodeStatistics stats : retrieved) {
            Assert.assertEquals(OCCURRENCE * 2, stats.getOccurrence());
        }
    }

    @Test
    public void testShouldProperlyUpdateNodeStatisticsWithAttributeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(true);

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID_1, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID_1);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : retrieved) {
            Assert.assertTrue(toStore.contains(stats));
            Assert.assertTrue(stats.hasAttributes());
        }
    }

    private NodeStatistics createNodeStatistics(String parentXpath, String nodeXpath, String nodeValue, long occurrence, boolean withAttributes) {
        NodeStatistics nodeStatistics = new NodeStatistics(parentXpath, nodeXpath, nodeValue, occurrence);
        if (withAttributes) {
            Set<AttributeStatistics> attributeStatistics = new HashSet<>();
            attributeStatistics.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_1_VALUE));
            nodeStatistics.setAttributesStatistics(attributeStatistics);
        }
        return nodeStatistics;
    }

    private List<NodeStatistics> prepareNodeStatistics(boolean withAttributes) {
        List<NodeStatistics> statistics = new ArrayList<>();
        statistics.add(createNodeStatistics("", ROOT_XPATH, "", OCCURRENCE, withAttributes));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_1, OCCURRENCE, withAttributes));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_2, OCCURRENCE, withAttributes));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_2_XPATH, NODE_VALUE_1, OCCURRENCE, withAttributes));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_3_XPATH, "", OCCURRENCE, withAttributes));
        statistics.add(createNodeStatistics(NODE_3_XPATH, NODE_4_XPATH, NODE_VALUE_3, OCCURRENCE, withAttributes));
        return statistics;
    }
}
