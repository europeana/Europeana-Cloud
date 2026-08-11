package eu.europeana.cloud.service.dps.storm.service;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.storm.dao.CassandraAttributeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.StatisticsReportDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class ValidationStatisticsServiceImplTest extends CassandraTestBase {

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

  private static final String NODE_1_VALUE = "value1";

    private static final String ATTRIBUTE_2_NAME = "attribute2";

    private static final long OCCURRENCE = 1;

    private ValidationStatisticsServiceImpl validationStatisticsService;

    private StatisticsReportDAO statisticsReportDAO;

    private CassandraAttributeStatisticsDAO attributeStatisticsDAO;

    @BeforeEach
    void setUp() {
        CassandraConnectionProvider cassandra = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                CassandraEnvironment.HOST, CassandraEnvironment.getPort(),
                KEYSPACE, "", "");
        statisticsReportDAO = StatisticsReportDAO.getInstance(cassandra);
        validationStatisticsService = ValidationStatisticsServiceImpl.getInstance(cassandra);
        attributeStatisticsDAO = CassandraAttributeStatisticsDAO.getInstance(cassandra);
    }

    @Test
    void testShouldProperlyStoreNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(null);

        // when
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = validationStatisticsService.getNodeStatistics(TASK_ID);
        assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : toStore) {
            assertTrue(retrieved.contains(stats));
    }
  }

    @Test
    void testShouldProperlyUpdateNodeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(null);

        // when
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = validationStatisticsService.getNodeStatistics(TASK_ID);
        assertEquals(retrieved.size(), toStore.size());
    for (NodeStatistics stats : toStore) {
        assertTrue(retrieved.contains(stats));
    }
    for (NodeStatistics stats : retrieved) {
        assertEquals(OCCURRENCE * 2, stats.getOccurrence());
    }
  }

    @Test
    void testShouldProperlyUpdateNodeStatisticsWithAttributeStatistics() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeStatistics> retrieved = validationStatisticsService.getNodeStatistics(TASK_ID);
        assertEquals(retrieved.size(), toStore.size());
        for (NodeStatistics stats : retrieved) {
            assertTrue(toStore.contains(stats));
            assertTrue(stats.hasAttributes());
        }
  }

  private List<AttributeStatistics> createAttributeStatistics() {
    List<AttributeStatistics> result = new ArrayList<>();
    result.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_1_VALUE));
    result.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_2_VALUE));
    return result;
  }

  private NodeStatistics createNodeStatistics(String parentXpath, String nodeXpath, String nodeValue, long occurrence,
      AttributeStatistics withAttributes) {
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
    statistics.add(
        createNodeStatistics("", ROOT_XPATH, "", OCCURRENCE, attributeStatistics != null ? attributeStatistics.get(0) : null));
    statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_1, OCCURRENCE,
        attributeStatistics != null ? attributeStatistics.get(0) : null));
    statistics.add(createNodeStatistics(ROOT_XPATH, NODE_1_XPATH, NODE_VALUE_2, OCCURRENCE,
        attributeStatistics != null ? attributeStatistics.get(1) : null));
    statistics.add(createNodeStatistics(ROOT_XPATH, NODE_2_XPATH, NODE_VALUE_1, OCCURRENCE,
        attributeStatistics != null ? attributeStatistics.get(0) : null));
    statistics.add(createNodeStatistics(ROOT_XPATH, NODE_3_XPATH, "", OCCURRENCE,
        attributeStatistics != null ? attributeStatistics.get(1) : null));
    statistics.add(createNodeStatistics(NODE_3_XPATH, NODE_4_XPATH, NODE_VALUE_3, OCCURRENCE,
        attributeStatistics != null ? attributeStatistics.get(0) : null));
    return statistics;
  }

    @Test
    void testShouldReturnFalseWhenReportNotPresent() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);

        // then
        assertFalse(statisticsReportDAO.isReportStored(TASK_ID));
    }

    @Test
    void testShouldStoreReportSuccessfully() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());
        StatisticsReport report = new StatisticsReport(TASK_ID, toStore);

        // when
        validationStatisticsService.storeStatisticsReport(TASK_ID, report);

        // then
        assertTrue(statisticsReportDAO.isReportStored(TASK_ID));
    }

    @Test
    void testShouldProperlyStoreAndRetrieveReport() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());
        StatisticsReport report = new StatisticsReport(TASK_ID, toStore);

        // when
        validationStatisticsService.storeStatisticsReport(TASK_ID, report);

        // then
        StatisticsReport reportRetrieved = statisticsReportDAO.getStatisticsReport(TASK_ID);
        assertEquals(report, reportRetrieved);
  }


    @Test
    void shouldProperlyReturnElementReportWithAttributes() {
        // given
        List<NodeStatistics> toStore = prepareNodeStatistics(createAttributeStatistics());

        // when
        validationStatisticsService.insertNodeStatistics(TASK_ID, toStore);

        // then
        List<NodeReport> nodeReportList = validationStatisticsService.getElementReport(TASK_ID, NODE_1_XPATH);
        assertNotNull(nodeReportList);
        assertEquals(2, nodeReportList.size());
        List<String> expectedValues = Arrays.asList(NODE_VALUE_1, NODE_VALUE_2);
        for (NodeReport nodeReport : nodeReportList) {
            assertTrue(expectedValues.contains(nodeReport.getNodeValue()));
            assertEquals(OCCURRENCE, nodeReportList.get(0).getOccurrence());
            assertNotNull(nodeReport.getAttributeStatistics());
            assertEquals(1, nodeReport.getAttributeStatistics().size());
        }
    }

    @Test
    void shouldGetTaskStatisticsReport() {
        // given
        List<NodeStatistics> stats = prepareStats();
        validationStatisticsService.insertNodeStatistics(TASK_ID, prepareStats());
        // when
        StatisticsReport actual = validationStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        assertNotNull(statisticsReportDAO.getStatisticsReport(TASK_ID));
        assertEquals(TASK_ID, actual.getTaskId());
        assertThat(actual.getNodeStatistics().size(), is(2));
    assertEquals(stats, actual.getNodeStatistics());
  }

    @Test
    void shouldGetTaskStatisticsReportManyTimes() {
        // given
        List<NodeStatistics> stats = prepareStats();
        validationStatisticsService.insertNodeStatistics(TASK_ID, prepareStats());
        // when
        StatisticsReport actual = validationStatisticsService.getTaskStatisticsReport(TASK_ID);
        assertNotNull(actual);
        actual = validationStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        assertNotNull(statisticsReportDAO.getStatisticsReport(TASK_ID));
    assertEquals(TASK_ID, actual.getTaskId());
    assertThat(actual.getNodeStatistics().size(), is(2));
    assertEquals(stats, actual.getNodeStatistics());
  }


    @Test
    void shouldNotThrowExceptionWhenReportNotExists() {
        // given
        // when
        StatisticsReport actual = validationStatisticsService.getTaskStatisticsReport(TASK_ID);

        // then
        assertNull(actual);
    }

    @Test
    void shouldProperlyRemoveStatistics() {
        // given
        validationStatisticsService.insertNodeStatistics(TASK_ID, prepareStats());
        StatisticsReport actual = validationStatisticsService.getTaskStatisticsReport(TASK_ID);
        assertNotNull(actual);
        // when
        validationStatisticsService.removeStatistics(TASK_ID);
        // tnen
        assertNull(validationStatisticsService.getTaskStatisticsReport(TASK_ID));


  }

  private List<NodeStatistics> prepareStats() {
    List<NodeStatistics> nodeStatistics = new ArrayList<>();
    NodeStatistics node1 = new NodeStatistics("parentXpath", "xpath", "value", 2L);
    NodeStatistics node2 = new NodeStatistics("parentXpath2", "xpath2", "value", 2L);
    nodeStatistics.add(node1);
    nodeStatistics.add(node2);
    return nodeStatistics;
  }

    @Test
    void testShouldProperlyStoreAttributeStatistics() {
        // given
        Set<AttributeStatistics> toStore = prepareAttributeStatistics();

        // when
        validationStatisticsService.insertAttributeStatistics(TASK_ID, NODE_1_XPATH, NODE_1_VALUE, toStore);

        // then
        Set<AttributeStatistics> retrieved = attributeStatisticsDAO.getAttributeStatistics(TASK_ID, NODE_1_XPATH, NODE_1_VALUE, 2);
        assertEquals(retrieved.size(), toStore.size());
        for (AttributeStatistics stats : toStore) {
            assertTrue(retrieved.contains(stats));
    }
  }

    @Test
    void testShouldProperlyUpdateAttributeStatistics() {
        // given
        Set<AttributeStatistics> toStore = prepareAttributeStatistics();

        // when
        validationStatisticsService.insertAttributeStatistics(TASK_ID, NODE_1_XPATH, NODE_1_VALUE, toStore);
        validationStatisticsService.insertAttributeStatistics(TASK_ID, NODE_1_XPATH, NODE_1_VALUE, toStore);

        // then
        Set<AttributeStatistics> retrieved = attributeStatisticsDAO.getAttributeStatistics(TASK_ID, NODE_1_XPATH, NODE_1_VALUE, 2);
        assertEquals(retrieved.size(), toStore.size());
    for (AttributeStatistics stats : toStore) {
        assertTrue(retrieved.contains(stats));
    }
    for (AttributeStatistics stats : retrieved) {
        assertEquals(2, stats.getOccurrence());
    }
  }

  private Set<AttributeStatistics> prepareAttributeStatistics() {
    Set<AttributeStatistics> statistics = new HashSet<>();
    statistics.add(new AttributeStatistics(ATTRIBUTE_1_NAME, ATTRIBUTE_1_VALUE));
    statistics.add(new AttributeStatistics(ATTRIBUTE_2_NAME, ATTRIBUTE_2_VALUE));
    return statistics;
  }
}
