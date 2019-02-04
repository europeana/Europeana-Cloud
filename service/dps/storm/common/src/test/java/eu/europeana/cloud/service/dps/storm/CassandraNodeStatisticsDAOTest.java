package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class CassandraNodeStatisticsDAOTest extends CassandraTestBase {
    private static final long TASK_ID = 1;

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

    private static final String ATTRIBUTE_2_VALUE = "value2";

    private static final long OCCURRENCE = 1;

    private CassandraNodeStatisticsDAO nodeStatisticsDAO;

    @Before
    public void setUp() {
        nodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST, PORT, KEYSPACE, "", ""));
    }

    @Test
    public void testShouldProperlyStoreNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(null);

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : toStore) {
            Assert.assertTrue(retrieved.contains(stats));
        }
    }

    @Test
    public void testShouldProperlyUpdateNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(null);

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID);
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
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = nodeStatisticsDAO.getNodeStatistics(TASK_ID);
        Assert.assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : retrieved) {
            Assert.assertTrue(toStore.contains(stats));
            Assert.assertTrue(stats.hasAttributes());
        }
    }

    private List<AttributeStatistics> createAttributeStatistics() {
        List<AttributeStatistics> result = new ArrayList<>();
        result.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_1_VALUE));
        result.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_2_VALUE));
        return result;
    }

    private NodeStatistics createNodeStatistics(String parentXpath, String nodeXpath, String nodeValue, long occurrence, AttributeStatistics withAttributes) {
        NodeStatistics nodeStatistics = new NodeStatistics(parentXpath, nodeXpath, nodeValue, occurrence);
        if (withAttributes != null) {
            Set<AttributeStatistics> attributeStatistics = new HashSet<>();
            attributeStatistics.add(withAttributes);
            nodeStatistics.setAttributesStatistics(attributeStatistics);
        }
        return nodeStatistics;
    }

    private List<NodeStatistics> prepareNodeStatistics(List<AttributeStatistics> attributeStatistics) {
        List<NodeStatistics> statistics = new ArrayList<>();
        statistics.add(createNodeStatistics("", ROOT_XPATH, "", OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(0) : null));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_1, OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(0) : null));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_2, OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(1) : null));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_2_XPATH, NODE_VALUE_1, OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(0) : null));
        statistics.add(createNodeStatistics(ROOT_XPATH, NODE_3_XPATH, "", OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(1) : null));
        statistics.add(createNodeStatistics(NODE_3_XPATH, NODE_4_XPATH, NODE_VALUE_3, OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(0) : null));
        return statistics;
    }

    @Test
    public void testShouldReturnFalseWhenReportNotPresent() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        Assert.assertFalse(nodeStatisticsDAO.isReportStored(TASK_ID));
    }

    @Test
    public void testShouldStoreReportSuccessfully() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());
        StatisticsReport report = new StatisticsReport(TASK_ID, toStore);

        // when
        nodeStatisticsDAO.storeStatisticsReport(TASK_ID, report);


        // then
        Assert.assertTrue(nodeStatisticsDAO.isReportStored(TASK_ID));
    }

    @Test
    public void testShouldProperlyStoreAndRetrieveReport() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());
        StatisticsReport report = new StatisticsReport(TASK_ID, toStore);

        // when
        nodeStatisticsDAO.storeStatisticsReport(TASK_ID, report);

        // then
        StatisticsReport reportRetrieved = nodeStatisticsDAO.getStatisticsReport(TASK_ID);
        Assert.assertEquals(report, reportRetrieved);
    }


    @Test
    public void shouldProperlyReturnElementReport() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        nodeStatisticsDAO.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeReport> nodeReportList = nodeStatisticsDAO.getElementReport(TASK_ID, NODE_1_XPATH);
        Assert.assertNotNull(nodeReportList);
        Assert.assertEquals(2, nodeReportList.size());
        List<String> expectedValues = Arrays.asList(NODE_VALUE_1, NODE_VALUE_2);
        for (NodeReport nodeReport : nodeReportList) {
            Assert.assertTrue(expectedValues.contains(nodeReport.getNodeValue()));
            Assert.assertEquals(OCCURRENCE, nodeReportList.get(0).getOccurrence());
        }
    }
}
